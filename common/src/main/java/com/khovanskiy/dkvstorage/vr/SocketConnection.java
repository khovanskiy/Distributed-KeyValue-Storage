package com.khovanskiy.dkvstorage.vr;


import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author Victor Khovanskiy
 * @deprecated
 */
public class SocketConnection {

    private final static int DISCONNECTED = 0;
    private final static int RECONNECTING = 1;
    private final static int CONNECTED = 2;

    private InetSocketAddress address;
    private Socket socket;
    private volatile ConnectionListener listener;
    private ExecutorService backgroundExecutor = Executors.newFixedThreadPool(3);
    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicInteger connected = new AtomicInteger(DISCONNECTED);
    private final ConnectionListener selfListener = new ConnectionListener() {
        @Override
        public void onConnected(SocketConnection node) {
            if (connected.compareAndSet(RECONNECTING, CONNECTED)) {
                if (listener != null) {
                    listener.onConnected(node);
                }
            }
        }

        @Override
        public void onReceived(SocketConnection node, @NotNull String line) {
            if (listener != null) {
                listener.onReceived(node, line);
            }
        }

        @Override
        public void onDisconnected(SocketConnection node, @Nullable IOException e) {
            if (connected.compareAndSet(CONNECTED, DISCONNECTED)) {
                running.set(false);
                if (listener != null) {
                    listener.onDisconnected(node, e);
                }
            }
        }
    };
    private AtomicBoolean keepConnection = new AtomicBoolean(false);
    private final Runnable runnableReconnector = new Runnable() {
        @Override
        public void run() {
            while (running.get() && keepConnection.get()) {
                while (connected.compareAndSet(DISCONNECTED, RECONNECTING)) {
                    //System.out.println("Try reconnect to " + address + "... " + keepConnection.get() + " " + connected.get());
                    try {
                        socket = new Socket(address.getAddress(), address.getPort());
                        updateRW();
                        selfListener.onConnected(SocketConnection.this);
                    } catch (IOException e) {
                        connected.set(DISCONNECTED);
                    }
                }
            }
        }
    };
    private BufferedReader reader;
    private final Runnable runnableReader = new Runnable() {
        @Override
        public void run() {
            while (running.get()) {
                if (keepConnection.get()) {
                    if (isConnected()) {
                        try {
                            //System.out.println(Connection.this.toString() + " try reading " + connected.get() + " - OUT");
                            tryReading();
                        } catch (IOException ignore) {
                            // ignore exception
                        } finally {
                            connected.compareAndSet(CONNECTED, DISCONNECTED);
                        }
                        // wait reconnection
                    }
                } else {
                    //System.out.println(Connection.this.toString() + " try reading " + connected.get() + " - IN");
                    try {
                        tryReading();
                    } catch (IOException exception) {
                        selfListener.onDisconnected(SocketConnection.this, exception);
                    } finally {
                        selfListener.onDisconnected(SocketConnection.this, null);
                    }
                    // connection failed -> leave loop
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }

        private void tryReading() throws IOException {
            String line;
            while ((line = reader.readLine()) != null) {
                //System.out.println("Recieved message \"" + line + "\"");
                selfListener.onReceived(SocketConnection.this, line);
            }
        }
    };
    private BufferedWriter writer;
    private LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();
    private final Runnable runnableWriter = new Runnable() {
        @Override
        public void run() {
            while (running.get()) {
                if (keepConnection.get()) {
                    if (isConnected()) {
                        try {
                            runWriting();
                        } catch (IOException ignore) {
                            // ignore exception
                        } finally {
                            connected.compareAndSet(CONNECTED, DISCONNECTED);
                        }
                        // wait reconnection
                    }
                } else {
                    try {
                        runWriting();
                    } catch (IOException exception) {
                        selfListener.onDisconnected(SocketConnection.this, exception);
                    } finally {
                        selfListener.onDisconnected(SocketConnection.this, null);
                    }
                    // connection failed -> leave loop
                }
            }
        }

        private void runWriting() throws IOException {
            while (running.get()) {
                String line;
                try {
                    System.out.println("Take and wait...");
                    line = messages.take();
                    System.out.println("Message \"" + line + "\" is sent");
                    writer.write(line);
                    writer.newLine();
                    writer.flush();
                } catch (InterruptedException ignore) {
                    // Ignore take() exception and try again
                }
            }

        }
    };

    public SocketConnection(String host, int port) {
        this.address = new InetSocketAddress(host, port);
        this.connected.set(DISCONNECTED);
    }

    public SocketConnection(InetSocketAddress address) {
        this.address = address;
        this.connected.set(DISCONNECTED);
    }

    public SocketConnection(Socket socket) {
        this.socket = socket;
        this.address = (InetSocketAddress) this.socket.getRemoteSocketAddress();
        this.connected.set(CONNECTED);
        updateRW();
    }

    private void updateRW() {
        try {
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (IOException e) {
        }
    }

    public String getHost() {
        return address.getHostName();
    }

    public int getPort() {
        return address.getPort();
    }

    public void setKeepConnection(boolean keepConnection) {
        this.keepConnection.set(keepConnection);
    }

    public void send(String message) {
        try {
            messages.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    @Override
    public String toString() {
        return getHost() + ":" + getPort();
    }

    public static abstract class ConnectionListener {

        public void onConnected(SocketConnection node) {

        }

        public void onReceived(SocketConnection node, @NotNull String line) {

        }

        public void onDisconnected(SocketConnection node, @Nullable IOException e) {

        }
    }
}
