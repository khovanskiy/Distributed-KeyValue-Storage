package com.khovanskiy.dkvstorage.server;

import com.khovanskiy.dkvstorage.vr.Node;
import com.khovanskiy.dkvstorage.vr.ServerNode;

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

    private ServerNode current;
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

                    if (id == nodeId) {
                        current = new ServerNode(id, host, port);
                        current.setId(id);
                    } else {
                        Node node = new Node(host, port);
                        node.setId(id);
                        nodes.add(node);
                    }

                }
            }
        }
        System.out.println("Other nodes:");
        for (Node node : nodes) {
            System.out.println(node);
        }
        System.out.println("Current node: " + current);
        current.start(nodes);

        current.broadcast("node " + current.getId());
    }
}
