package com.khovanskiy.dkvstorage.client;

import com.khovanskiy.dkvstorage.vr.Connection;
import com.khovanskiy.dkvstorage.vr.message.MessageHandler;
import com.khovanskiy.dkvstorage.vr.message.MessageRequest;
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
        Connection connection = new Connection("localhost", 721);
        connection.setKeepConnection(true);
        connection.setConnectionListener(new Connection.ConnectionListener() {
            @Override
            public void onConnected(Connection node) {
                System.out.println("Connected to server");
            }

            @Override
            public void onReceived(Connection node, @NotNull String line) {
                System.out.println("Server response: " + line);
            }

            @Override
            public void onDisconnected(Connection node, @Nullable IOException e) {
                System.out.println("Disconnected from server");
            }
        });
        connection.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        int clientId = 5;
        int requestNumber = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null || line.equals("q")) {
                connection.stop();
                return;
            }
            MessageHandler parser = new MessageHandler(line);
            try {
                Operation operation = parser.parseOperation();
                MessageRequest request = new MessageRequest(operation, clientId, requestNumber);
                System.out.println("Send request: " + request);
                connection.send(request.toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
