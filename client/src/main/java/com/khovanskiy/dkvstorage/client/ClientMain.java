package com.khovanskiy.dkvstorage.client;

import com.khovanskiy.dkvstorage.vr.Network;
import com.khovanskiy.dkvstorage.vr.SocketConnection;
import com.khovanskiy.dkvstorage.vr.message.MessageHandler;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;

/**
 * @author Victor Khovanskiy
 */
public class ClientMain {

    public static void main(String[] args) throws IOException {
        new ClientMain().execute();
    }

    private void execute() throws IOException {
        Network network = new Network();

        int serverConnection = network.connect("localhost", 222, true);
        network.setConnectionListener(new Network.ConnectionListener() {
            @Override
            public void onConnected(int connectionId) {
                System.out.println("Connected to server");
            }

            @Override
            public void onDisconnected(int connectionId) {
                System.out.println("Disconnected from server");
            }

            @Override
            public void onReceived(int connectionId, String line) {
                System.out.println("Server response: " + line);
            }
        });
        network.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line = reader.readLine();
            if (line == null || line.equals("q")) {
                network.stop();
                return;
            }
            network.send(serverConnection, line);
            /*MessageHandler parser = new MessageHandler(line);
            try {
                Operation operation = parser.parseOperation();
                System.out.println("Send request: " + operation);
                network.send(serverConnection, operation.toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }*/
        }
    }
}
