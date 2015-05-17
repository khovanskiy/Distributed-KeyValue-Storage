package com.khovanskiy.dkvstorage.server;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Victor Khovanskiy
 */
public class Node {

    private class IncomingRunnable implements Runnable {
        private Socket connection;

        public IncomingRunnable(Socket socket) throws IOException {
            this.connection = socket;
        }

        @Override
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                while (true) {

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class ManagerRunnable implements Runnable {
        private ServerSocket connection;

        public ManagerRunnable(int port) throws IOException {
            connection = new ServerSocket(port);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    System.out.println("Waiting incoming connections...");
                    Socket socket = connection.accept();
                    System.out.println("Socket " + socket + " connected");
                    new Thread(new IncomingRunnable(socket)).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class OutcomingRunnable implements Runnable {
        private Socket connection;
        private Node node;

        public OutcomingRunnable(Node node) throws IOException {
            this.node = node;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    //System.out.println("Trying to create outcoming connection to " + node + "...");
                    connection = new Socket(node.address.getHostName(), node.address.getPort());
                    System.out.println("Created outcoming connection to " + node);
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
                    while (true) {
                        writer.write("hello from " + Node.this.id);
                    }
                } catch (IOException e) {
                    //System.out.println(e);
                    //System.out.println("Failed");
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private int id;
    private InetSocketAddress address;
    private Map<String, String> storage = new HashMap<>();

    public Node(int id, String host, int port) {
        this.id = id;
        this.address = new InetSocketAddress(host, port);
    }

    public void execute(List<Node> nodes) throws IOException {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(new ManagerRunnable(address.getPort()));
        for (Node node : nodes) {
            if (node.id != id) {
                runnables.add(new OutcomingRunnable(node));
            }
        }
        for (Runnable runnable : runnables) {
            Thread thread = new Thread(runnable);
            thread.start();
        }
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "#" + id + " = " + address.getHostName() + ":" + address.getPort();
    }
}
