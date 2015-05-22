package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.IdentificationMessage;
import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.message.MessageHandler;
import com.khovanskiy.dkvstorage.vr.message.MessageReply;
import com.khovanskiy.dkvstorage.vr.message.PrepareMessage;
import com.khovanskiy.dkvstorage.vr.message.PrepareOkMessage;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.IOException;
import java.net.Socket;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Victor Khovanskiy
 */
public class Replica {

    private class Client {
        private int clientId;
        private int connectionId;

        public Client(int clientId, int connectionId) {
            this.clientId = clientId;
            this.connectionId = connectionId;
        }

        public int getClientId() {
            return clientId;
        }

        public int getConnectionId() {
            return connectionId;
        }
    }

    private final ExecutorService backgroundExecutor = Executors.newCachedThreadPool();
    private final Looper looper = new Looper();
    private final Network.ConnectionListener connectionListener = new Network.ConnectionListener() {
        @Override
        public void onConnected(int connectionId) {
            looper.run(new Runnable() {
                @Override
                public void run() {
                    Replica.this.onConnected(connectionId);
                }
            });
        }

        @Override
        public void onDisconnected(int connectionId) {
            looper.run(new Runnable() {
                @Override
                public void run() {
                    Replica.this.onDisconnected(connectionId);
                }
            });
        }

        @Override
        public void onAccept(int connectionId) {
            looper.run(new Runnable() {
                @Override
                public void run() {
                    Replica.this.onAccepted(connectionId);
                }
            });
        }

        @Override
        public void onReceived(int connectionId, String line) {
            Replica.this.onReceived(connectionId, line);
        }
    };

    /**
     * This is a sorted array containing the 2f + 1 replicas.
     */
    private List<Replica> configuration;
    /**
     * This is the index into the configuration where this replica is stored.
     */
    private final int replicaNumber;
    /**
     * The current view-number, initially 0.
     */
    private int viewNumber = 0;
    /**
     * The current status, either @code{ReplicaStatus.NORMAL}, @code{ReplicaStatus.VIEW_CHANGE}, or @code{ReplicaStatus.RECOVERING}
     */
    private ReplicaStatus status;
    /**
     * This is assigned to the most recently received request, initially 0.
     */
    private int opNumber = 0;
    /**
     * This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
     */
    private final List<LogEntry> log = new ArrayList<>();
    /**
     * This is the op-number of the most recently committed operation.
     */
    private int commitNumber;
    /**
     * This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
     */
    private final Map<Integer, ClientEntry> clientTable = new HashMap<>();


    private final Map<Integer, Map<Integer, Boolean>> ballotTable = new HashMap<>();
    private int quorumSize;

    //private Logger logger = Logger.getLogger(Replica.class.getName());
    private int primaryNumber;
    private final String host;
    private final int port;
    private Set<Integer> outgoing;
    private Set<Integer> incoming;
    private Set<Integer> incomingClients;
    private Set<Integer> incomingReplicas;
    private Network network;
    public Map<String, String> storage = new HashMap<>();

    public Replica(int replicaNumber, String host, int port) throws IOException {
        this.replicaNumber = replicaNumber;
        this.host = host;
        this.port = port;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public void start(List<Replica> configuration) throws IOException {
        trace("Replica " + toString() + " starting...");
        this.quorumSize = configuration.size() / 2 + 1;
        this.status = ReplicaStatus.NORMAL;
        this.primaryNumber = configuration.get(0).getReplicaNumber();
        this.configuration = configuration;
        this.network = new Network(getHost(), getPort());

        this.outgoing = new HashSet<>(this.configuration.size() - 1);
        this.incoming = new HashSet<>(this.configuration.size());

        trace("Creating outgoing connections to other replicas: ");
        int k = 1;
        for (Replica replica : configuration) {
            if (replica.getReplicaNumber() == getReplicaNumber()) {
                continue;
            }
            int connectionId = network.connect(replica.getHost(), replica.getPort(), true);
            outgoing.add(connectionId);
            trace(k++ + ") Created connection to " + replica);
        }

        network.setConnectionListener(connectionListener);
        network.start();

        trace("Launch processing looper");
        backgroundExecutor.submit(looper);
    }

    private void trace(String s) {
        System.out.println(s);
    }

    private void onConnected(int connectionId) {
        trace("Connected to remote #" + connectionId);
        sendTo(connectionId, new IdentificationMessage(getReplicaNumber()));
    }

    private void onDisconnected(int connectionId) {
        trace("Disconnected #" + connectionId);
    }

    private void onAccepted(int connectionId) {
        trace("Accepted #" + connectionId);
        incoming.add(connectionId);
    }

    private void onReceived(int connectionId, String line) {
        trace("Received from #" + connectionId + ": " + line);
        MessageHandler parser = new MessageHandler(line);
        Message message;
        try {
            message = parser.parse();
        } catch (ParseException exception) {
            System.out.println("Invalid message: " + exception.getMessage());
            return;
        }
        // TODO: replace this workaround
        if (message instanceof IdentificationMessage) {
            onReceivedIdentification(connectionId, (IdentificationMessage) message);
        } else if (message instanceof RequestMessage) {

            onReceivedRequest((RequestMessage) message);
        } else if (message instanceof PrepareMessage) {
            onReceivedPrepare((PrepareMessage) message);
        } else if (message instanceof PrepareOkMessage) {
            onReceivedPrepareOk((PrepareOkMessage) message);
        }
    }

    private void sendTo(int connectionId, Message message) {
        network.send(connectionId, message.toString());
    }

    private void sendOtherReplicas(Message message) {
        for (int connectionId : outgoing) {
            network.send(connectionId, message.toString());
        }
    }

    public void onReceivedIdentification(int connectionId, IdentificationMessage identification) {
        if (incoming.contains(connectionId)) {
            incoming.remove(connectionId);
            if (incomingReplicas.contains(connectionId)) {
                network.disconnect(connectionId);
            }
            sendTo(connectionId, new MessageReply("ACCEPTED"));
        }
    }

    public void onReceivedRequest(RequestMessage request) {
        if (!isPrimary()) {
            return;
        }

        ClientEntry entry = clientTable.get(request.getClientId());
        if (entry != null) {
            // If the request-number s isn’t bigger than the information in the table
            if (request.getRequestNumber() < entry.getRequest().getRequestNumber()) {
                // drops stale request
                return;
            }
            // if the request is the most recent one from this client and it has already been executed
            if (request.getRequestNumber() == entry.getRequest().getRequestNumber()) {
                if (entry.isProcessing()) {
                    // still processing request
                    return;
                } else {
                    // re-send the response
                    replyClient(request.getClientId(), entry.getReply());
                }
            }
        }

        // advances op-number
        opNumber++;

        // adds the request to the end of the log
        log.add(new LogEntry(request));

        // updates the information for this client in the client-table to contain the new request number
        clientTable.put(request.getClientId(), new ClientEntry(request));

        // prepare table for ballot by voting backups
        ballotTable.put(opNumber, new HashMap<>());

        // sends a <PREPARE v, m, n, k> message to the other replicas
        Message prepare = new PrepareMessage(viewNumber, request, opNumber, commitNumber);
        sendOtherReplicas(prepare);
    }

    public void replyClient(int clientId, MessageReply reply) {
        // TODO
    }

    public void onReceivedPrepare(PrepareMessage prepare) {
        if (prepare.getViewNumber() > viewNumber) {
            //If this node also thinks it is a prmary, will do recovery
            //go s.StartRecovery()
            return;
        }

        // TODO: Handle msg from older views
        if (prepare.getViewNumber() < viewNumber) {
            return;
        }

        if (prepare.getOpNumber() > opNumber + 1) {
            //go s.StartRecovery()
            return;
        }

        // Ignore out-of-order message
        if (prepare.getOpNumber() < opNumber + 1) {
            return;
        }

        // won’t accept a prepare with op-number n until it has entries for all earlier requests in its log.
        commitUpTo(prepare.getCommitNumber());

        //increments its op-number
        opNumber++;

        // adds the request to the end of its log
        log.add(new LogEntry(prepare.getRequest()));

        clientTable.put(prepare.getRequest().getClientId(), new ClientEntry(prepare.getRequest()));
    }

    public void onReceivedPrepareOk(PrepareOkMessage prepareOk) {
        // Ignore commited operations
        if (prepareOk.getOpNumber() <= commitNumber) {
            return;
        }

        // Update table for PrepareOK messages
        Map<Integer, Boolean> ballot = ballotTable.get(prepareOk.getOpNumber());
        if (ballot == null) {
            return;
        }
        ballot.put(prepareOk.getBackupNumber(), true);

        // Commit operations agreed by quorum
        if (prepareOk.getOpNumber() > commitNumber + 1) {
            return;
        }

        while (commitNumber < opNumber) {
            ballot = ballotTable.get(commitNumber + 1);
            if (ballot.size() + 1 >= quorumSize) {
                executeNextOp();
                ballotTable.remove(commitNumber);
            } else {
                break;
            }
        }
    }

    private void executeNextOp() {
        RequestMessage request = log.get(commitNumber + 1).getRequest();
        clientTable.get(request.getClientId()).setReply(upCall(request.getOperation()));
        clientTable.get(request.getClientId()).setProcessing(false);
        ++commitNumber;
    }

    private void commitUpTo(int primaryCommit) {
        while (commitNumber < primaryCommit && commitNumber < opNumber) {
            executeNextOp();
        }
    }

    private MessageReply upCall(Operation operation) {
        trace(operation + " " + "is executed");
        return operation.delegateUpCall(this);
    }

    public boolean isPrimary() {
        return getReplicaNumber() == primaryNumber;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return getHost() + ":" + getPort();
    }
}
