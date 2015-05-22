package com.khovanskiy.dkvstorage.vr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Victor Khovanskiy
 */
public class Network {

    private final Selector selector;
    private final LinkedList<Request> pendingChanges = new LinkedList<>();
    private final Map<Integer, Connection> connections = new HashMap<>();
    private final ExecutorService backgroundExecutor = Executors.newSingleThreadExecutor();
    private final Runnable runnable = new Runnable() {
        @Override
        public void run() {
            while (true) {
                //System.out.println("loop");
                synchronized (pendingChanges) {
                    while (!pendingChanges.isEmpty()) {
                        Request request = pendingChanges.poll();
                        Connection connection = request.getConnection();
                        switch (request.getType()) {
                            case CONNECT: {
                                reconnect(connection);
                            }
                            break;
                            case CHANGEOPS: {
                                SelectionKey key = connection.getKey();
                                int ops = request.getOps();
                                if ((key.interestOps() & SelectionKey.OP_CONNECT) == 0) {
                                    key.interestOps(ops);
                                }
                            }
                            break;
                            case DISCONNECT: {
                                connection.setKeepConnection(false);
                                reconnect(connection);
                            }
                            break;
                        }
                    }
                    pendingChanges.clear();
                }
                try {
                    int readyKeys = selector.select();
                    if (readyKeys == 0) {
                        continue;
                    }
                    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        SelectionKey key = keys.next();
                        keys.remove();
                        if (!key.isValid()) {
                            continue;
                        }
                        if (key.isAcceptable()) {
                            onAcceptable(key);
                        } else if (key.isConnectable()) {
                            onConnectable(key);
                        } else if (key.isReadable()) {
                            onReadable(key);
                        } else if (key.isWritable()) {
                            onWritable(key);
                        }
                    }
                } catch (IOException ignored) {
                    ignored.printStackTrace();
                }
            }
        }
    };
    private final ByteBuffer readBuffer = ByteBuffer.allocate(8192);
    private final InetSocketAddress address;
    private ConnectionListener listener = new ConnectionListener();
    private int nextConnectionId = 0;

    public Network(String host, int port) throws IOException {
        this.address = new InetSocketAddress(host, port);
        this.selector = Selector.open();
    }

    private void onAcceptable(SelectionKey key) {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        try {
            SocketChannel client = server.accept();
            client.configureBlocking(false);
            Connection connection;
            connection = nextConnection((InetSocketAddress) client.getRemoteAddress(), false);
            SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ, connection);
            connection.setKey(clientKey);
            listener.onAccept(connection.getId());
        } catch (IOException ignored) {
        }
    }

    private void onConnectable(SelectionKey key) {
        Connection connection = (Connection) key.attachment();
        try {
            SocketChannel remote = (SocketChannel) key.channel();
            remote.finishConnect();
            onConnected(connection);
        } catch (IOException ignored) {
            onDisconnected(connection);
        }
    }

    private void onConnected(Connection connection) {
        synchronized (connection.getQueue()) {
            if (!connection.getQueue().isEmpty()) {
                connection.getKey().interestOps(SelectionKey.OP_WRITE);
            } else {
                connection.getKey().interestOps(SelectionKey.OP_READ);
            }
        }
        listener.onConnected(connection.getId());
    }

    private void onDisconnected(Connection connection) {
        SelectionKey key = connection.getKey();
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            channel.close();
        } catch (IOException ignored) {
        }
        key.cancel();
        if (connection.isKeepConnection()) {
            reconnect(connection);
        }
    }

    private void onReadable(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Connection connection = (Connection) key.attachment();
        readBuffer.clear();
        int readCount;
        try {
            readCount = channel.read(readBuffer);
        } catch (IOException e) {
            listener.onDisconnected(connection.getId());
            onDisconnected(connection);
            return;
        }
        if (readCount == -1) {
            listener.onDisconnected(connection.getId());
            onDisconnected(connection);
            return;
        }
        // TODO: improve line separation
        String medley = new String(readBuffer.array(), 0, readCount, StandardCharsets.UTF_8);
        String[] lines = medley.split("\\r?\\n");
        for (String line : lines) {
            listener.onReceived(connection.getId(), line);
        }
    }

    private synchronized void onWritable(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Connection connection = (Connection) key.attachment();
        Queue<ByteBuffer> queue = connection.getQueue();
        synchronized (queue) {
            while (!queue.isEmpty()) {
                ByteBuffer buffer = queue.peek();
                channel.write(buffer);
                if (buffer.remaining() > 0) {
                    break;
                }
                queue.poll();
            }
            if (queue.isEmpty()) {
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    public void start() throws IOException {
        ServerSocketChannel channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        channel.bind(address);
        channel.register(selector, SelectionKey.OP_ACCEPT);
        backgroundExecutor.submit(runnable);
    }

    public void send(int connectionId, String line) {
        Connection connection = getConnection(connectionId);
        Queue<ByteBuffer> queue = connection.getQueue();
        ByteBuffer buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8));
        synchronized (queue) {
            queue.add(buffer);
        }
        synchronized (pendingChanges) {
            pendingChanges.add(new Request(connection, RequestType.CHANGEOPS, SelectionKey.OP_WRITE));
        }
    }

    public int connect(String host, int port, boolean keepConnection) {
        Connection connection = nextConnection(host, port, keepConnection);
        synchronized (pendingChanges) {
            pendingChanges.add(new Request(connection, RequestType.CONNECT, SelectionKey.OP_CONNECT));
        }
        return connection.getId();
    }

    public void disconnect(int connectionId) {
        Connection connection = getConnection(connectionId);
        if (connection == null) {
            return;
        }
        synchronized (pendingChanges) {
            pendingChanges.add(new Request(connection, RequestType.DISCONNECT, 0));
        }
    }

    private Connection nextConnection(String host, int port, boolean keepConnection) {
        return nextConnection(new InetSocketAddress(host, port), keepConnection);
    }

    private Connection nextConnection(InetSocketAddress address, boolean keepConnection) {
        synchronized (connections) {
            Connection connection = new Connection(nextConnectionId, address);
            connection.setKeepConnection(keepConnection);
            connections.put(nextConnectionId, connection);
            nextConnectionId++;
            return connection;
        }
    }

    private Connection getConnection(int connectionId) {
        synchronized (connections) {
            return connections.get(connectionId);
        }
    }

    private void reconnect(Connection connection) {
        System.out.println("Try reconnect " + connection);
        try {
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT, connection);
            connection.setKey(key);
            channel.connect(connection.getAddress());
        } catch (IOException ignored) {
        }
    }

    public void setConnectionListener(ConnectionListener listener) {
        this.listener = listener;
    }

    private enum RequestType {
        REGISTER, DISCONNECT, CONNECT, CHANGEOPS
    }

    public static class ConnectionListener {

        public void onAccept(int connectionId) {
            System.out.println("Accepted " + connectionId);
        }

        public void onConnected(int connectionId) {
            System.out.println("Connected to " + connectionId);
        }

        public void onReceived(int connectionId, String line) {
            System.out.println("Received from [" + connectionId + "]: " + line);
        }

        public void onDisconnected(int connectionId) {
            System.out.println("Disconnected from " + connectionId);
        }
    }

    private class Request {
        private final RequestType type;
        private final Connection connection;
        private final int ops;

        public Request(Connection connection, RequestType type, int ops) {
            this.connection = connection;
            this.type = type;
            this.ops = ops;
        }

        public RequestType getType() {
            return type;
        }

        public Connection getConnection() {
            return connection;
        }

        public int getOps() {
            return ops;
        }
    }

    private class Connection {
        private final int id;
        private final LinkedList<ByteBuffer> queue = new LinkedList<>();
        private final InetSocketAddress address;
        private boolean keepConnection;
        private SelectionKey key;

        public Connection(int connectionId, String host, int port) {
            this.id = connectionId;
            this.address = new InetSocketAddress(host, port);
        }

        public Connection(int connectionId, InetSocketAddress address) {
            this.id = connectionId;
            this.address = address;
        }

        public int getId() {
            return id;
        }

        public LinkedList<ByteBuffer> getQueue() {
            return queue;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        public boolean isKeepConnection() {
            return keepConnection;
        }

        public void setKeepConnection(boolean keepConnection) {
            this.keepConnection = keepConnection;
        }

        public String getHost() {
            return address.getHostName();
        }

        public int getPort() {
            return address.getPort();
        }

        @Override
        public String toString() {
            return getHost() + ":" + getPort();
        }

        private SelectionKey getKey() {
            return key;
        }

        private void setKey(SelectionKey key) {
            this.key = key;
        }
    }
}
