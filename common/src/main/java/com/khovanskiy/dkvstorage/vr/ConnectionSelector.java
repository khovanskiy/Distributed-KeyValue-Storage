package com.khovanskiy.dkvstorage.vr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: vvvVI_000
 * Date: 21.05.2015
 * Time: 12:01
 */
public class ConnectionSelector {

    private final Selector selector;
    private final ExecutorService backgroundExecutor = Executors.newSingleThreadExecutor();
    private final Runnable runnable = new Runnable() {
        @Override
        public void run() {
            while (true) {
                try {
                    int readyKeys = selector.select();
                    if (readyKeys == 0) {
                        continue;
                    }
                    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        SelectionKey key = keys.next();
                        keys.remove();
                        if (key.isAcceptable()) {
                            ServerSocketChannel server = (ServerSocketChannel) key.channel();
                            SocketChannel client = server.accept();
                            client.configureBlocking(false);
                            Connection connection = new Connection((InetSocketAddress) client.getRemoteAddress());
                            key.attach(connection);
                            listener.onAccept(connection);
                            client.register(selector, SelectionKey.OP_READ);
                        } else if (key.isConnectable()) {
                            Connection connection = (Connection) key.attachment();
                            try {
                                SocketChannel remote = (SocketChannel) key.channel();
                                remote.finishConnect();
                                if (remote.isConnected()) {
                                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                                    listener.onConnected(connection);
                                }
                            } catch (IOException ignored) {
                                ignored.printStackTrace();
                                if (connection.isKeepConnection()) {
                                    reconnect(connection);
                                } else {
                                    listener.onDisconnected(connection);
                                }
                            }
                        } else if (key.isReadable()) {
                            System.out.println("Read " + key);
                        } else if (key.isWritable()) {
                            System.out.println("Write " + key);
                            SocketChannel remote = (SocketChannel) key.channel();
                            Connection connection = (Connection) key.attachment();
                            while (!connection.queue.isEmpty()) {
                                //remote.write()
                            }
                        }
                    }
                } catch (IOException ignored) {
                }
            }
        }
    };

    private ConnectionListener listener = new ConnectionListener();

    public ConnectionSelector(String host, int port) throws IOException {
        this.selector = Selector.open();
    }

    public void start() {
        backgroundExecutor.submit(runnable);
    }

    public boolean connect(Connection connection) {
        return reconnect(connection);
    }

    private boolean reconnect(Connection connection) {
        System.out.println("Try reconnect " + connection);
        try {
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT);
            key.attach(connection);
            channel.connect(connection.getAddress());
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    public static class Connection {
        private boolean keepConnection;

        public LinkedList<String> getQueue() {
            return queue;
        }

        private LinkedList<String> queue = new LinkedList<>();

        public Connection(String host, int port) {
            this.address = new InetSocketAddress(host, port);
        }

        public Connection(InetSocketAddress address) {
            this.address = address;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        private InetSocketAddress address;

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
    }

    public static class ConnectionListener {

        public void onAccept(Connection connection) {
            System.out.println("Accepted " + connection);
        }

        public void onConnected(Connection connection) {
            System.out.println("Connected to " + connection);
        }

        public void onReceived(Connection connection, String line) {

        }

        public void onDisconnected(Connection connection) {
            System.out.println("Disconnected from " + connection);
        }
    }
}
