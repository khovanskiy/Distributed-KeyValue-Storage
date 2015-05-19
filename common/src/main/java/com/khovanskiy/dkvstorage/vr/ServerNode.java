package com.khovanskiy.dkvstorage.vr;


import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerNode extends Node {

    private Map<Integer, Node> nodes = new HashMap<>();
    private List<Node> unknowns = new ArrayList<>();
    private Map<Integer, Node> clients = new HashMap<>();
    private ExecutorService backgroundExecutor = Executors.newSingleThreadExecutor();
    private ServerSocket serverSocket;

    private final ConnectionListener connectionListener = new ConnectionListener() {

        @Override
        public void onConnected(Node node) {
            System.out.println("Connected " + node);
        }

        @Override
        public void onDisconnected(Node node, @Nullable IOException e) {
            System.out.println("Disconnected " + node);
        }

        @Override
        public void onReceived(Node node, @NotNull String line) {
            System.out.println("Receive line from " + node + ": " + line);
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
        super(host, port);
        serverSocket = new ServerSocket(port);
    }

    public void start(Collection<Node> nodes) {
        for (Node node : nodes) {
            node.setConnectionListener(connectionListener);
            this.nodes.put(node.getId(), node);
        }
        for (Node node : nodes) {
            node.start();
        }
        backgroundExecutor.execute(runnableAccepter);
    }

    public synchronized void broadcast(String m) {
        for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
            if (entry.getKey() != getId()) {
                entry.getValue().send(m);
            }
        }
    }

    protected synchronized void onAccept(@NotNull Socket socket) {
        Node node = new Node(socket);
        unknowns.add(node);
        node.setConnectionListener(connectionListener);
        node.start();
        node.send("hello from " + getId());
    }
}
