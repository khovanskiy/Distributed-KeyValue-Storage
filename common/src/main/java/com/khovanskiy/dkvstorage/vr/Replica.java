package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.*;
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
     * This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
     */
    private final Map<Integer, ClientEntry> clientTable = new HashMap<>();
    private final Map<Long, Map<Integer, Boolean>> ballotRequestTable = new HashMap<>();
    private final String host;
    private final int port;
    private final Map<Integer, Map<Integer, Boolean>> ballotPrimaryTable = new HashMap<>();
    private final ViewChangeState viewChangeState = new ViewChangeState(this);
    private final RecoveryState recoveryState = new RecoveryState(this);
    private final Wrapper wrapper = new Wrapper(this);
    /**
     * Local key-value storage
     */
    public Map<String, String> storage = new HashMap<>();
    /**
     * This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
     */
    private ReplicaLog log = new ReplicaLog();
    /**
     * This is a sorted array containing the 2f + 1 replicas.
     */
    private List<Replica> configuration;
    /**
     * The current view-number, initially 0.
     */
    private long viewNumber = 0;
    /**
     * The current status, either @code{ReplicaStatus.NORMAL}, @code{ReplicaStatus.VIEW_CHANGE}, or @code{ReplicaStatus.RECOVERING}
     */
    private ReplicaStatus status;
    /**
     * This is assigned to the most recently received request, initially 0.
     */
    private long operationNumber = 0;
    /**
     * This is the op-number of the most recently committed operation.
     */
    private long commitNumber;
    private int quorumSize;
    private int primaryNumber;
    private long lastTick;

    public Replica(int replicaNumber, String host, int port) throws IOException {
        this.replicaNumber = replicaNumber;
        this.host = host;
        this.port = port;
    }

    public long getOperationNumber() {
        return operationNumber;
    }

    public void setOperationNumber(long operationNumber) {
        this.operationNumber = operationNumber;
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    public void setCommitNumber(long commitNumber) {
        this.commitNumber = commitNumber;
    }

    public Wrapper getWrapper() {
        return wrapper;
    }

    public ReplicaLog getLog() {
        return log;
    }

    public void setLog(ReplicaLog log) {
        this.log = log;
    }

    public long getViewNumber() {
        return viewNumber;
    }

    public void setViewNumber(long viewNumber) {
        this.viewNumber = viewNumber;
    }

    public ReplicaStatus getStatus() {
        return status;
    }

    public void setStatus(ReplicaStatus status) {
        this.status = status;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public void start(int timeout, List<Replica> configuration) throws IOException {
        trace("Replica " + toString() + " starting...");
        this.configuration = configuration;
        this.quorumSize = configuration.size() / 2 + 1;
        this.status = ReplicaStatus.NORMAL;
        this.primaryNumber = configuration.get(0).getReplicaNumber();
        wrapper.start(timeout);
    }

    private void clear() {
        this.viewNumber = 0;
        this.commitNumber = 0;
        this.operationNumber = 0;
        this.status = ReplicaStatus.NORMAL;
        this.log.clear();
    }

    public void stop() throws IOException {
        trace("Replica " + toString() + " stopping...");
        wrapper.stop();
        clear();
    }

    private void trace(String s) {
        System.out.println("{REPLICA " + getReplicaNumber() + "} = " + s);
    }

    @NotNull
    private ClientEntry getClient(int clientId) {
        ClientEntry entry = clientTable.get(clientId);
        if (entry == null) {
            entry = new ClientEntry(clientId);
            clientTable.put(clientId, entry);
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
        operationNumber++;

        // adds the request to the end of the log
        log.put(operationNumber, request);

        // updates the information for this client in the client-table to contain the new request number
        entry.setRequestNumber(request.getRequestNumber());
        entry.setProcessing(true);

        // prepare table for ballot by voting backups
        ballotRequestTable.put(operationNumber, new HashMap<>());

        // sends a <PREPARE v, m, n, k> message to the other replicas
        Message prepare = new PrepareMessage(request, viewNumber, operationNumber, commitNumber);
        wrapper.sendToOtherReplicas(prepare);
    }

    public void onReceivedPrepare(PrepareMessage message) {
        // Replica state is not NORMAL. Ignoring PREPARE
        if (getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        if (message.getViewNumber() > viewNumber) {
            recoveryState.startRecovery();
            return;
        }

        if (message.getOperationNumber() > operationNumber + 1) {
            recoveryState.startRecovery();
            return;
        }

        // TODO: Handle msg from older views
        if (message.getViewNumber() < viewNumber) {
            return;
        }

        // Ignore out-of-order message
        if (message.getOperationNumber() < operationNumber + 1) {
            return;
        }

        // won’t accept a prepare with op-number n until it has entries for all earlier requests in its log.
        commitUpTo(message.getCommitNumber());

        //increments its op-number
        operationNumber++;

        // adds the request to the end of its log
        log.put(operationNumber, message.getRequest());

        //  updates the client's information in the client-table
        ClientEntry entry = getClient(message.getRequest().getClientId());
        entry.setRequestNumber(message.getRequest().getRequestNumber());

        // and sends a [PREPAREOK v, n, i] message to the primary to indicate that this operation and all earlier ones have prepared locally.
        PrepareOkMessage prepareOk = new PrepareOkMessage(viewNumber, operationNumber, getReplicaNumber());
        wrapper.sendToPrimary(prepareOk);
    }

    public void onReceivedPrepareOk(PrepareOkMessage prepareOk) {
        // Replica state is not NORMAL. Ignoring PREPARE_OK
        if (getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        // Ignore committed operations
        if (prepareOk.getOperationNumber() <= commitNumber) {
            return;
        }

        // Update table for PrepareOK messages
        Map<Integer, Boolean> ballot = ballotRequestTable.get(prepareOk.getOperationNumber());
        if (ballot == null) {
            return;
        }
        ballot.put(prepareOk.getReplicaNumber(), true);

        // Commit operations agreed by quorum
        if (prepareOk.getOperationNumber() > commitNumber + 1) {
            return;
        }

        boolean changed = false;
        while (commitNumber < operationNumber) {
            ballot = ballotRequestTable.get(commitNumber + 1);
            int f = ballot.size();
            // The primary waits for f PREPAREOK messages from different backups;
            if (f >= configuration.size() / 2) {
                // at this point it considers the operation (and all earlier ones) to be committed.
                RequestMessage request = log.get(commitNumber + 1);

                // Then, after it has executed all earlier operations (those assigned smaller op-numbers), the primary executes the operation by making an up-call
                String result = upCall(request.getOperation());

                // and increments its commit-number.
                ++commitNumber;
                changed = true;

                // The primary also updates the client's entry in the client-table to contain the result.
                ClientEntry entry = getClient(request.getClientId());
                entry.setResult(result);
                entry.setProcessing(false);

                // It sends a [REPLY v, s, x] message to the client.
                ReplyMessage reply = new ReplyMessage(viewNumber, request.getRequestNumber(), entry.getResult());
                wrapper.sendToClient(request.getClientId(), reply);

                ballotRequestTable.remove(commitNumber);
            } else {
                break;
            }
        }

        // informs the backups of the latest commit by sending them a [COMMIT v, k] message
        if (changed) {
            wrapper.sendToOtherReplicas(new CommitMessage(viewNumber, commitNumber));
        }
    }

    public void onReceivedCommit(CommitMessage message) {
        // Replica state is not NORMAL. Ignoring COMMIT
        if (getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        if (message.getViewNumber() > viewNumber) {
            recoveryState.startRecovery();
            return;
        }

        // Ignore committed operations
        if (message.getCommitNumber() <= commitNumber) {
            return;
        }

        commitUpTo(message.getCommitNumber());
    }

    public void onReceivedReply(ReplyMessage reply) {
        wrapper.forwardReply(reply);
    }

    private void executeNextOp() {
        RequestMessage request = log.get(commitNumber + 1);
        ClientEntry entry = getClient(request.getClientId());
        entry.setResult(upCall(request.getOperation()));
        entry.setProcessing(false);
        ++commitNumber;
    }

    private void commitUpTo(long primaryCommit) {
        while (commitNumber < primaryCommit && commitNumber < operationNumber) {
            executeNextOp();
        }
    }

    private String upCall(Operation operation) {
        trace("\"" + operation + "\" " + "is executed");
        return operation.delegateUpCall(this);
    }

    public void onPrimaryDisconnected() {
        Utils.log(getReplicaNumber(), "Notice that primary disconnected");
        // notices the need for a view change advances its view-number
        viewChangeState.startViewChange();
        /*long newViewNumber = getViewNumber() + 1;
        if (status == ReplicaStatus.RECOVERING) {
            return;
        }
        if (status == ReplicaStatus.NORMAL) {
            setStatus(ReplicaStatus.VIEW_CHANGE);
            viewChangeState.reset(newViewNumber);
            wrapper.sendToOtherReplicas(new StartViewChangeMessage(newViewNumber, getReplicaNumber()));
        }*/
    }

    public void onReceivedStartViewChange(StartViewChangeMessage message) {
        viewChangeState.processViewChangeMessage(message);
    }

    public void onReceivedDoViewChange(DoViewChangeMessage message) {
        viewChangeState.processDoViewChangeMessage(message);
    }

    public void onReceivedStartView(StartViewMessage message) {
        viewChangeState.processStartViewMessage(message);
    }

    public void onReceivedRecovery(RecoveryMessage message) {
        recoveryState.processRecoveryMessage(message);
    }

    public void onReceivedRecoveryResponse(RecoveryResponseMessage message) {
        recoveryState.processRecoveryResponseMessage(message);
    }

    public boolean isPrimary() {
        return getReplicaNumber() == getPrimaryNumber();
    }

    public int getPrimaryNumber() {
        int offset = (int) (viewNumber % configuration.size());
        return configuration.get(offset).getReplicaNumber();
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

    public long getLastTick() {
        return lastTick;
    }

    public void setLastTick(long lastTick) {
        this.lastTick = lastTick;
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
