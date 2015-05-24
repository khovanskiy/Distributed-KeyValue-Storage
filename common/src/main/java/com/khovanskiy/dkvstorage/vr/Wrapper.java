package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;
import com.khovanskiy.dkvstorage.vr.operation.DeleteOperation;
import com.khovanskiy.dkvstorage.vr.operation.GetOperation;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.khovanskiy.dkvstorage.vr.operation.SetOperation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Wrapper {
    private final Replica currentReplica;
    private final Map<Integer, Integer> replicaToConnection = new HashMap<>();
    private final Map<Long, Client> requestToClient = new HashMap<>();
    private final Map<Integer, Client> clients = new HashMap<>();
    private final ExecutorService backgroundExecutor = Executors.newCachedThreadPool();
    private final Looper looper = new Looper();
    private final Timer timer = new Timer();
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
    private final TimerTask timerTask = new TimerTask() {
        @Override
        public void run() {
            looper.run(new Runnable() {
                @Override
                public void run() {
                    Wrapper.this.onTimerTick();
                }
            });
        }
    };
    private Map<Integer, Replica> replicas = new HashMap<>();
    private Network network;
    private long requestNumber;
    private int timeout;
    private long currentTimerTicks = 0;

    public Wrapper(Replica replica) {
        this.currentReplica = replica;
    }

    public void start(int timeout) throws IOException {
        this.timeout = timeout;

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

        // TODO
        // timer.scheduleAtFixedRate(timerTask, 0, timeout / 2);
    }

    public void stop() throws IOException {
        network.stop();
        timer.cancel();
    }

    public void sendToReplica(int replicaId, Message message) {
        sendToConnection(replicaToConnection.get(replicaId), message);
    }

    public void sendToPrimary(Message message) {
        sendToReplica(currentReplica.getPrimaryNumber(), message);
    }

    public void sendToClient(int clientId, ReplyMessage reply) {
        sendToClient(getClient(clientId), reply);
    }

    public void forwardReply(ReplyMessage reply) {
        sendToClient(requestToClient.get(reply.getRequestNumber()), reply);
    }

    protected void sendToClient(Client client, ReplyMessage reply) {
        int connectionId = client.getConnectionId();
        if (client.isReplica()) {
            sendToConnection(connectionId, reply);
        } else {
            sendToConnection(connectionId, reply.getResult());
        }
    }

    public void sendToConnection(int connectionId, Message message) {
        sendToConnection(connectionId, Message.encode(message).toString());
    }

    protected void sendToConnection(int connectionId, String message) {
        network.send(connectionId, message);
    }

    public void sendToOtherReplicas(Message message) {
        sendToOtherReplicas(Message.encode(message).toString());
    }

    protected void sendToOtherReplicas(String message) {
        for (Map.Entry<Integer, Integer> entry : replicaToConnection.entrySet()) {
            if (currentReplica.getReplicaNumber() != entry.getKey()) {
                sendToConnection(entry.getValue(), message);
            }
        }
    }

    private void onTimerTick() {
        if (currentTimerTicks % 2 == 0) {
            Map<Integer, Replica> newNodes = new HashMap<>(replicas.size());
            for (Map.Entry<Integer, Replica> entry : replicas.entrySet()) {
                int connectionId = entry.getKey();
                Replica replica = entry.getValue();
                if (Math.abs(currentTimerTicks - replica.getLastTick()) > 1) {
                    Utils.log(currentReplica.getReplicaNumber(), "Force disconnect from " + network.dump(connectionId));
                    network.disconnect(connectionId);

                    int newConnectionId = network.connect(replica.getHost(), replica.getPort(), true);

                    newNodes.put(newConnectionId, replica);
                    replicaToConnection.put(replica.getReplicaNumber(), newConnectionId);
                } else {
                    newNodes.put(connectionId, replica);
                }
            }
            replicas = newNodes;
        }

        sendToOtherReplicas("ping");
        ++currentTimerTicks;
    }

    private void onConnected(int connectionId) {
        Utils.log(currentReplica.getReplicaNumber(), "Connected to remote " + network.dump(connectionId));
        sendToConnection(connectionId, "node " + currentReplica.getReplicaNumber());
        Replica replica = replicas.get(connectionId);
        replica.setLastTick(currentTimerTicks);
    }

    private void onDisconnected(int connectionId) {
        System.out.println("Disconnected " + network.dump(connectionId));
        Replica replica = replicas.get(connectionId);
        if (replica != null) {
            if (currentReplica.getPrimaryNumber() == replica.getReplicaNumber()) {
                currentReplica.onPrimaryDisconnected();
            }
        }
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
            System.out.println("Parse error + " + line);
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
                processPong(connectionId);
                return true;
            case "primary": {
                if (slices.length != 1)
                    return false;
                sendToConnection(connectionId, "leader = " + currentReplica.getPrimaryNumber());
                return true;
            }
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
        /*if (currentReplica.getReplicaNumber() == 1) {
            if (currentTimerTicks > 3 && currentTimerTicks < 10 || currentTimerTicks > 30 && currentTimerTicks < 50){
                return;
            }
        }*/
        sendToConnection(connectionId, "pong");
    }

    private void processPong(int connectionId) {
        Replica replica = replicas.get(connectionId);
        if (replica != null) {
            replica.setLastTick(currentTimerTicks);
        }
    }

    private Client getClient(int clientId) {
        Client client = clients.get(clientId);
        if (client == null) {
            client = new Client(clientId);
            clients.put(clientId, client);
        }
        return client;
    }

    private class Node {
        private int nodeId;
        private long lastTick;

        public Node(int nodeid) {
            this.nodeId = nodeid;
        }

        public int getNodeId() {
            return this.nodeId;
        }

        public long getLastTick() {
            return this.lastTick;
        }

        public void setLastTick(long lastTick) {
            this.lastTick = lastTick;
        }
    }

    private class Client {
        private int clientId;
        private int requestNumber;
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

        public int getConnectionId() {
            return this.connectionId;
        }

        public void setConnectionId(int connectionId) {
            this.connectionId = connectionId;
        }
    }
}
