package com.khovanskiy.dkvstorage.vr;


import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerNode {

    private final Map<Integer, SocketConnection> nodes = new HashMap<>();
    private final List<SocketConnection> unknowns = new ArrayList<>();
    private final Map<Integer, SocketConnection> clients = new HashMap<>();
    private final ExecutorService backgroundExecutor = Executors.newSingleThreadExecutor();
    private ServerSocket serverSocket;

    private final SocketConnection.ConnectionListener connectionListener = new SocketConnection.ConnectionListener() {

        @Override
        public void onConnected(SocketConnection node) {
            System.out.println("Connected [" + node + "]");
        }

        @Override
        public void onDisconnected(SocketConnection node, @Nullable IOException e) {
            System.out.println("Disconnected [" + node + "]");
        }

        @Override
        public void onReceived(SocketConnection node, @NotNull String line) {
            System.out.println("Receive line from [" + node + "]: " + line);
        }
    };

    private final Runnable runnableAccepter = new Runnable() {
        @Override
        public void run() {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    onAccept(socket);
                } catch (IOException e) {
                }
            }
        }
    };

    public ServerNode(int id, String host, int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void start(Collection<SocketConnection> nodes) {
        for (SocketConnection node : nodes) {
            node.setKeepConnection(true);
            node.setConnectionListener(connectionListener);
            //this.nodes.put(node.getId(), node);
        }
        for (SocketConnection node : nodes) {
            node.start();
        }
        backgroundExecutor.execute(runnableAccepter);
    }

    public synchronized void broadcast(String m) {
        for (Map.Entry<Integer, SocketConnection> entry : nodes.entrySet()) {
            /*if (entry.getKey() != getId()) {
                entry.getValue().send(m);
            }*/
        }
    }

    protected synchronized void onAccept(@NotNull Socket socket) {
        SocketConnection node = new SocketConnection(socket);
        unknowns.add(node);
        node.setConnectionListener(connectionListener);
        node.start();
        //node.send("hello from " + getId());
    }
}
