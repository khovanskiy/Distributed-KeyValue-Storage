package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;
import com.khovanskiy.dkvstorage.vr.message.PrepareMessage;
import com.khovanskiy.dkvstorage.vr.message.PrepareOkMessage;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.sun.istack.internal.NotNull;

import java.io.IOException;
import java.util.*;

/**
 * @author Victor Khovanskiy
 */
public class Replica {
    /**
     * This is the index into the configuration where this replica is stored.
     */
    private final int replicaNumber;
    /**
     * This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
     */
    private final Map<Integer, LogEntry> log = new HashMap<>();
    /**
     * This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
     */
    private final Map<Integer, ClientEntry> clientTable = new HashMap<>();
    private final Map<Integer, Map<Integer, Boolean>> ballotTable = new HashMap<>();
    private final String host;
    private final int port;
    public Map<String, String> storage = new HashMap<>();
    /**
     * This is a sorted array containing the 2f + 1 replicas.
     */
    private List<Replica> configuration;
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
     * This is the op-number of the most recently committed operation.
     */
    private int commitNumber;
    private int quorumSize;
    private int primaryNumber;

    private Wrapper wrapper = new Wrapper(this);

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
        wrapper.start();
    }

    private void trace(String s) {
        System.out.println("{REPLICA " + getReplicaNumber() + "} = " + s);
    }


    @NotNull
    private ClientEntry getClient(int clientId) {
        ClientEntry entry = clientTable.get(clientId);
        if (entry == null) {
            entry = new ClientEntry(clientId);
        }
        return entry;
    }

    public void onReceivedRequest(RequestMessage request) {
        if (!isPrimary()) {
            return;
        }

        ClientEntry entry = getClient(request.getClientId());

        // If the request-number s isn’t bigger than the information in the table
        if (request.getRequestNumber() < entry.getRequestNumber()) {
            // drops stale request
            return;
        }
        // if the request is the most recent one from this client and it has already been executed
        if (request.getRequestNumber() == entry.getRequestNumber()) {
            if (entry.isProcessing()) {
                // still processing request
                return;
            } else {
                // re-send the response
                ReplyMessage reply = new ReplyMessage(viewNumber, request.getRequestNumber(), entry.getResult());
                wrapper.sendToClient(request.getClientId(), reply);
            }
        }

        // advances op-number
        opNumber++;

        // adds the request to the end of the log
        log.put(opNumber, new LogEntry(request));

        // updates the information for this client in the client-table to contain the new request number
        entry.setRequestNumber(request.getRequestNumber());
        entry.setProcessing(true);

        // prepare table for ballot by voting backups
        ballotTable.put(opNumber, new HashMap<>());

        // sends a <PREPARE v, m, n, k> message to the other replicas
        Message prepare = new PrepareMessage(request, viewNumber, opNumber, commitNumber);
        wrapper.sendToOtherReplicas(prepare);
    }

    public void onReceivedPrepare(PrepareMessage prepare) {
        if (prepare.getViewNumber() > viewNumber) {
            //If this node also thinks it is a primary, will do recovery
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
        log.put(opNumber, new LogEntry(prepare.getRequest()));

        //  updates the client's information in the client-table
        ClientEntry entry = getClient(prepare.getRequest().getClientId());
        entry.setRequestNumber(prepare.getRequest().getRequestNumber());

        // and sends a [PREPAREOK v, n, i] message to the primary to indicate that this operation and all earlier ones have prepared locally.
        PrepareOkMessage prepareOk = new PrepareOkMessage(viewNumber, opNumber, getReplicaNumber());
        wrapper.sendToPrimary(prepareOk);
    }

    public void onReceivedPrepareOk(PrepareOkMessage prepareOk) {
        // Ignore committed operations
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
            int f = ballot.size();
            // The primary waits for f PREPAREOK messages from different backups;
            if (f >= quorumSize) {
                // at this point it considers the operation (and all earlier ones) to be committed.
                RequestMessage request = log.get(commitNumber + 1).getRequest();

                // Then, after it has executed all earlier operations (those assigned smaller op-numbers), the primary executes the operation by making an up-call
                String result = upCall(request.getOperation());

                // and increments its commit-number.
                ++commitNumber;

                // The primary also updates the client's entry in the client-table to contain the result.
                ClientEntry entry = getClient(request.getClientId());
                entry.setResult(result);
                entry.setProcessing(false);

                // It sends a [REPLY v, s, x] message to the client.
                ReplyMessage reply = new ReplyMessage(viewNumber, request.getRequestNumber(), entry.getResult());
                wrapper.sendToClient(request.getClientId(), reply);

                ballotTable.remove(commitNumber);
            } else {
                break;
            }
        }
    }

    public void onReceivedReply(ReplyMessage reply) {
        wrapper.forwardReply(reply);
    }

    private void executeNextOp() {
        RequestMessage request = log.get(commitNumber + 1).getRequest();
        ClientEntry entry = getClient(request.getClientId());
        entry.setResult(upCall(request.getOperation()));
        entry.setProcessing(false);
        ++commitNumber;
    }

    private void commitUpTo(int primaryCommit) {
        while (commitNumber < primaryCommit && commitNumber < opNumber) {
            executeNextOp();
        }
    }

    private String upCall(Operation operation) {
        trace("\"" + operation + "\" " + "is executed");
        return operation.delegateUpCall(this);
    }

    public boolean isPrimary() {
        return getReplicaNumber() == getPrimaryNumber();
    }

    public int getPrimaryNumber() {
        return primaryNumber;
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

    public List<Replica> getConfiguration() {
        return configuration;
    }

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
}
