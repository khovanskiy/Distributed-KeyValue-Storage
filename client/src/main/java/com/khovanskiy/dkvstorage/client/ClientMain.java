package com.khovanskiy.dkvstorage.client;

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
        SocketConnection socketConnection = new SocketConnection("localhost", 111);
        socketConnection.setKeepConnection(true);
        socketConnection.setConnectionListener(new SocketConnection.ConnectionListener() {
            @Override
            public void onConnected(SocketConnection node) {
                System.out.println("Connected to server");
            }

            @Override
            public void onReceived(SocketConnection node, @NotNull String line) {
                System.out.println("Server response: " + line);
            }

            @Override
            public void onDisconnected(SocketConnection node, @Nullable IOException e) {
                System.out.println("Disconnected from server");
            }
        });
        socketConnection.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        int clientId = 5;
        int requestNumber = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null || line.equals("q")) {
                socketConnection.stop();
                return;
            }
            MessageHandler parser = new MessageHandler(line);
            try {
                Operation operation = parser.parseOperation();
                RequestMessage request = new RequestMessage(operation, clientId, requestNumber);
                System.out.println("Send request: " + request);
                socketConnection.send(request.toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
