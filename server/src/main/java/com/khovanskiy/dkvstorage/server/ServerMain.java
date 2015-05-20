package com.khovanskiy.dkvstorage.server;

import com.khovanskiy.dkvstorage.vr.Connection;
import com.khovanskiy.dkvstorage.vr.Replica;
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

    private Replica current;
    private int timeout = DEFAULT_TIMEOUT;

    public static void main(String[] args) throws IOException {
        new ServerMain().execute(args);
    }

    public void execute(String[] args) throws IOException {
        int replicaNumber;
        if (args.length >= 2 && args[0].equals("dkvs_node")) {
            replicaNumber = Integer.parseInt(args[1]);
        } else {
            return;
        }
        List<Replica> configuration = new ArrayList<>();
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

                    Replica replica = new Replica(id, host, port);
                    if (id == replicaNumber) {
                        current = replica;
                    }
                    configuration.add(replica);
                }
            }
        }

        System.out.println("Current node: " + current);
        System.out.println("Other nodes:");
        for (Replica replica : configuration) {
            System.out.println(replica);
        }
        current.start(configuration);
    }
}
