package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;
import com.khovanskiy.dkvstorage.vr.operation.DeleteOperation;
import com.khovanskiy.dkvstorage.vr.operation.GetOperation;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.khovanskiy.dkvstorage.vr.operation.SetOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Wrapper {
    private final Replica currentReplica;
    private final Map<Integer, Integer> replicaToConnection = new HashMap<>();
    private final Map<Integer, Replica> replicas = new HashMap<>();
    private final Map<Integer, Client> requestToClient = new HashMap<>();
    private final Map<Integer, Client> clients = new HashMap<>();
    private final ExecutorService backgroundExecutor = Executors.newCachedThreadPool();
    private final Looper looper = new Looper();
    private final Network.ConnectionListener connectionListener = new Network.ConnectionListener() {
        @Override
        public void onConnected(int connectionId) {
            looper.run(new Runnable() {
                @Override
                public void run() {
                    Wrapper.this.onConnected(connectionId);
                }
            });
        }

        @Override
        public void onDisconnected(int connectionId) {
            looper.run(new Runnable() {
                @Override
                public void run() {
                    Wrapper.this.onDisconnected(connectionId);
                }
            });
        }

        @Override
        public void onAccept(int connectionId) {
            looper.run(new Runnable() {
                @Override
                public void run() {
                    Wrapper.this.onAccepted(connectionId);
                }
            });
        }

        @Override
        public void onReceived(int connectionId, String line) {
            Wrapper.this.onReceived(connectionId, line);
        }
    };
    private Network network;
    private int requestNumber;

    public Wrapper(Replica replica) {
        this.currentReplica = replica;
    }

    public void start() throws IOException {
        network = new Network();
        for (Replica anotherReplica : currentReplica.getConfiguration()) {
            if (anotherReplica.getReplicaNumber() == currentReplica.getReplicaNumber()) {
                continue;
            }
            int connectionId = network.connect(anotherReplica.getHost(), anotherReplica.getPort(), true);
            replicaToConnection.put(anotherReplica.getReplicaNumber(), connectionId);
            replicas.put(connectionId, anotherReplica);
        }
        network.setConnectionListener(connectionListener);
        network.server(currentReplica.getHost(), currentReplica.getPort());
        network.start();
        backgroundExecutor.submit(looper);
    }

    public void sendToReplica(int replicaId, Message message) {
        sendToConnection(replicaToConnection.get(replicaId), message);
    }

    public void sendToPrimary(Message message) {
        sendToReplica(currentReplica.getPrimaryNumber(), message);
    }

    public void sendToClient(int clientId, ReplyMessage reply) {
        sendToConnection(getClient(clientId).getConnectionId(), reply);
    }

    public void forwardReply(ReplyMessage reply) {
        Client client = requestToClient.get(reply.getRequestNumber());
        int connectionId = client.getConnectionId();
        if (client.isReplica()) {
            sendToConnection(connectionId, reply);
        } else {
            sendToConnection(connectionId, reply.getResult());
        }
    }

    public void sendToConnection(int connectionId, Message message) {
        sendToConnection(connectionId, Message.encode(message));
    }

    protected void sendToConnection(int connectionId, String message) {
        network.send(connectionId, message);
    }

    public void sendToOtherReplicas(Message message) {
        for (Map.Entry<Integer, Integer> entry : replicaToConnection.entrySet()) {
            if (currentReplica.getReplicaNumber() != entry.getKey()) {
                sendToConnection(entry.getValue(), message);
            }
        }
    }

    private void onConnected(int connectionId) {
        Utils.log(currentReplica.getReplicaNumber(), "Connected to remote " + network.dump(connectionId));
        sendToConnection(connectionId, "node " + currentReplica.getReplicaNumber());
    }

    private void onDisconnected(int connectionId) {
        System.out.println("Disconnected " + network.dump(connectionId));
    }

    private void onAccepted(int connectionId) {
        Utils.log(currentReplica.getReplicaNumber(), "Accepted " + network.dump(connectionId));
        //incoming.add(connectionId);
    }

    private void onReceived(int connectionId, String line) {
        Utils.log(currentReplica.getReplicaNumber(), "Received from " + network.dump(connectionId) + ": " + line);
        try {
            if (processMessage(connectionId, line)) {
                return;
            }
            Message message = Message.decode(line);
            message.delegateProcessing(currentReplica);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean processMessage(int connectionId, String line) {
        String[] slices = line.trim().toLowerCase(Locale.US).split(" +");
        if (slices.length == 0)
            return false;
        switch (slices[0]) {
            case "node": {
                if (slices.length != 2) {
                    return false;
                }
                processNode(connectionId, Utils.parseInt(slices[1]));
                return true;
            }
            case "get":
                if (slices.length != 2) {
                    return false;
                }
                processClientRequest(new GetOperation(slices[1]), connectionId);
                return true;
            case "set":
                if (slices.length != 3) {
                    return false;
                }
                processClientRequest(new SetOperation(slices[1], slices[2]), connectionId);
                return true;
            case "delete":
                if (slices.length != 2) {
                    return false;
                }
                processClientRequest(new DeleteOperation(slices[1]), connectionId);
                return true;
            case "ping":
                if (slices.length != 1) {
                    return false;
                }
                processPing(connectionId);
                return true;
            case "pong":
                if (slices.length != 1)
                    return false;
                return true;
            case "accepted":
                return true;
        }
        return false;
    }

    private void processNode(int connectionId, int clientId) {
        Client client = getClient(clientId);
        if (client.hasConnectionId()) {
            int oldConnectionId = client.getConnectionId();
            network.disconnect(oldConnectionId);
        }
        client.setConnectionId(connectionId);
        client.markAsReplica();
        sendToConnection(connectionId, "ACCEPTED");
        /*if (incoming.contains(connectionId)) {
            incoming.remove(connectionId);
            Integer oldConnectionId = incomingReplicas.get(identification.getId());
            if (oldConnectionId != null) {
                incomingReplicas.remove(identification.getId());
                network.disconnect(oldConnectionId);
            }
            incomingReplicas.put(identification.getId(), connectionId);
            //sendTo(connectionId, new ReplyMessage("ACCEPTED"));
        }*/
    }

    private void processClientRequest(Operation operation, int connectionId) {
        Client client = getClient(connectionId);
        client.setConnectionId(connectionId);
        if (currentReplica.isPrimary()) {
            RequestMessage request = client.nextRequest(operation);
            currentReplica.onReceivedRequest(request);
        } else {
            ++requestNumber;
            RequestMessage request = new RequestMessage(operation, currentReplica.getReplicaNumber(), requestNumber);
            sendToPrimary(request);
            requestToClient.put(requestNumber, client);
        }
    }

    private void processPing(int connectionId) {
        sendToConnection(connectionId, "pong");
    }

    private Client getClient(int clientId) {
        Client client = clients.get(clientId);
        if (client == null) {
            client = new Client(clientId);
            clients.put(clientId, client);
        }
        return client;
    }

    private class Client {
        public int clientId;
        public int requestNumber;
        private int connectionId;
        private boolean hasConnectionId = false;
        private boolean isReplica = false;

        public Client(int clientId) {
            this.clientId = clientId;
        }

        public RequestMessage nextRequest(Operation operation) {
            ++requestNumber;
            return new RequestMessage(operation, clientId, requestNumber);
        }

        public void markAsReplica() {
            this.isReplica = true;
        }

        public boolean isReplica() {
            return this.isReplica;
        }

        public boolean hasConnectionId() {
            return this.hasConnectionId;
        }

        public void setConnectionId(int connectionId) {
            this.connectionId = connectionId;
        }

        public int getConnectionId() {
            return this.connectionId;
        }
    }
}
