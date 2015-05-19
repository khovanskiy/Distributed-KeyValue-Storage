package com.khovanskiy.dkvstorage.vr;


import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    private enum NodeType {
        INCOMING, OUTCOMING
    }

    private final static int DISCONNECTED = 0;
    private final static int RECONNECTING = 1;
    private final static int CONNECTED = 2;

    private int id = -1;
    private final NodeType type;
    private InetSocketAddress address;
    private Socket socket;
    private volatile ConnectionListener listener;
    private ExecutorService backgroundExecutor = Executors.newFixedThreadPool(3);
    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicInteger connected = new AtomicInteger(DISCONNECTED);
    private AtomicBoolean keepConnection = new AtomicBoolean(false);
    private LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();

    private final ConnectionListener selfListener = new ConnectionListener() {
        @Override
        public void onConnected(Node node) {
            if (connected.compareAndSet(RECONNECTING, CONNECTED)) {
                if (listener != null) {
                    listener.onConnected(node);
                }
            }
        }

        @Override
        public void onReceived(Node node, @NotNull String line) {
            if (listener != null) {
                listener.onReceived(node, line);
            }
        }

        @Override
        public void onDisconnected(Node node, @Nullable IOException e) {
            if (connected.compareAndSet(CONNECTED, DISCONNECTED)) {
                running.set(false);
                if (listener != null) {
                    listener.onDisconnected(node, e);
                }
            }
        }
    };

    private final Runnable runnableReconnector = new Runnable() {
        @Override
        public void run() {
            while (keepConnection.get()) {
                while (connected.compareAndSet(DISCONNECTED, RECONNECTING)) {
                    System.out.println("Try reconnect to " + address + "... " + keepConnection.get() + " " + connected.get());
                    try {
                        socket = new Socket(address.getAddress(), address.getPort());
                        selfListener.onConnected(Node.this);
                    } catch (IOException e) {
                        connected.set(DISCONNECTED);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    };

    private final Runnable runnableReader = new Runnable() {
        @Override
        public void run() {
            try {
                while (true) {
                    if (keepConnection.get()) {
                        if (isConnected()) {
                            try {
                                System.out.println("Try reading " + connected.get());
                                tryReading();
                            } catch (IOException exception) {
                                selfListener.onDisconnected(Node.this, exception);
                            } finally {
                                selfListener.onDisconnected(Node.this, null);
                            }
                            // wait reconnection
                        }
                    } else {
                        tryReading();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            } catch (IOException exception) {
                selfListener.onDisconnected(Node.this, exception);
            } finally {
                selfListener.onDisconnected(Node.this, null);
            }
        }

        private void tryReading() throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                selfListener.onReceived(Node.this, line);
            }
        }
    };

    private final Runnable runnableWriter = new Runnable() {
        @Override
        public void run() {
            try {
                while (running.get()) {
                    if (keepConnection.get()) {
                        if (isConnected()) {
                            try {
                                tryWriting();
                            } catch (IOException exception) {
                                selfListener.onDisconnected(Node.this, exception);
                            } finally {
                                selfListener.onDisconnected(Node.this, null);
                            }
                            // wait reconnection
                        }
                    } else {
                        tryWriting();
                    }
                }
            } catch (IOException exception) {
                selfListener.onDisconnected(Node.this, exception);
            } finally {
                selfListener.onDisconnected(Node.this, null);
            }
        }

        private void tryWriting() throws IOException {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            while (true) {
                String line;
                try {
                    line = messages.take();
                    writer.write(line);
                    writer.newLine();
                    writer.flush();
                } catch (InterruptedException ignore) {
                    // Ignore take() exception and try again
                }
            }

        }
    };

    public Node(String host, int port) {
        this.address = new InetSocketAddress(host, port);
        this.type = NodeType.OUTCOMING;
        this.connected.set(DISCONNECTED);
        this.keepConnection.set(true);
    }

    public Node(Socket socket) {
        this.socket = socket;
        this.type = NodeType.INCOMING;
        this.connected.set(CONNECTED);
    }

    public String getHost() {
        return address.getHostName();
    }

    public int getPort() {
        return address.getPort();
    }

    public void send(String message) {
        try {
            messages.put(message);
        } catch (InterruptedException e) {
        }
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            backgroundExecutor.execute(runnableReconnector);
            backgroundExecutor.execute(runnableReader);
            backgroundExecutor.execute(runnableWriter);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isConnected() {
        return connected.get() == CONNECTED;
    }

    public void stop() {
        running.set(false);
    }

    public void setConnectionListener(@Nullable ConnectionListener listener) {
        this.listener = listener;
    }

    public static abstract class ConnectionListener {

        public void onConnected(Node node) {

        }

        public void onReceived(Node node, @NotNull String line) {

        }

        public void onDisconnected(Node node, @Nullable IOException e) {

        }
    }

    @Override
    public String toString() {
        if (type == NodeType.OUTCOMING) {
            return "OUT " + getId() + " / " + getHost() + " : " + getPort();
        } else {
            return "IN " + getId();
        }
    }
}
