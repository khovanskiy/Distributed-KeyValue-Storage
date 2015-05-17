package com.khovanskiy.dkvstorage.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Victor Khovanskiy
 */
public class ServerMain {
    private static final String DEFAULT_CONFIG_FILENAME = "dkvs.properties";
    private static final int DEFAULT_TIMEOUT = 1000;

    private Node current;
    private int timeout = DEFAULT_TIMEOUT;
    private List<Node> nodes = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        new ServerMain().execute(args);
    }

    public void execute(String[] args) throws IOException {
        int nodeId;
        if (args.length >= 2 && args[0].equals("dkvs_node")) {
            nodeId = Integer.parseInt(args[1]);
        } else {
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(DEFAULT_CONFIG_FILENAME))) {
            while (reader.ready()) {
                String[] line = reader.readLine().split("=");
                if (line[0].equals("timeout")) {
                    timeout = Integer.parseInt(line[1]);
                } else {
                    String[] left = line[0].split("\\.");
                    String[] right = line[1].split(":");
                    int id = Integer.parseInt(left[1]);
                    String host = right[0];
                    int port = Integer.parseInt(right[1]);
                    Node node = new Node(id, host, port);
                    if (id == nodeId) {
                        current = node;
                    }
                    nodes.add(node);
                }
            }
        }
        for (Node node : nodes) {
            System.out.println(node);
        }
        System.out.println("Start " + current);
        current.execute(nodes);
    }
}
