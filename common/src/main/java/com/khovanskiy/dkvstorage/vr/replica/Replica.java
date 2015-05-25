package com.khovanskiy.dkvstorage.vr.replica;

import com.khovanskiy.dkvstorage.vr.*;
import com.khovanskiy.dkvstorage.vr.message.*;
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
    /**
     * This is host assigned to this replica
     */
    private final String host;
    /**
     * This is port assigned to this replica
     */
    private final int port;
    /**
     * State with delegating rights to handle NORMAL protocol
     */
    private final NormalState normalState = new NormalState(this);
    /**
     * State with delegating rights to handle VIEW_CHANGE protocol
     */
    private final ViewChangeState viewChangeState = new ViewChangeState(this);
    /**
     * State with delegating rights to handle RECOVERY protocol
     */
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
    private long lastTimestamp;
    private int timeout;

    public Replica(int replicaNumber, String host, int port) throws IOException {
        this.replicaNumber = replicaNumber;
        this.host = host;
        this.port = port;
    }

    public long getOperationNumber() {
        return operationNumber;
    }

    void setOperationNumber(long operationNumber) {
        this.operationNumber = operationNumber;
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    void setCommitNumber(long commitNumber) {
        this.commitNumber = commitNumber;
    }

    Wrapper getWrapper() {
        return wrapper;
    }

    ReplicaLog getLog() {
        return log;
    }

    void setLog(ReplicaLog log) {
        this.log = log;
    }

    public long getViewNumber() {
        return viewNumber;
    }

    void setViewNumber(long viewNumber) {
        this.viewNumber = viewNumber;
    }

    public ReplicaStatus getStatus() {
        return status;
    }

    void setStatus(ReplicaStatus status) {
        this.status = status;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public void start(int timeout, List<Replica> configuration) throws IOException {
        trace("Replica " + toString() + " starting...");
        this.timeout = timeout;
        this.configuration = configuration;
        this.status = ReplicaStatus.NORMAL;
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
    ClientEntry getClient(int clientId) {
        ClientEntry entry = clientTable.get(clientId);
        if (entry == null) {
            entry = new ClientEntry(clientId);
            clientTable.put(clientId, entry);
        }
        return entry;
    }

    void startRecovery() {
        recoveryState.startRecovery();
    }

    public void onReceivedTimeout() {
        // Normally the primary informs backups about the commit when it sends the next PREPARE message
        if (Math.abs(getLastTimestamp() - System.currentTimeMillis()) <= timeout) {
            return;
        }

        // However, if the primary does not receive a new client request in a timely way
        if (isPrimary()) {
            // it instead informs the backups of the latest commit by sending them a [COMMIT v, k] message
            wrapper.sendToOtherReplicas(new CommitMessage(getViewNumber(), getCommitNumber()));
        }
    }

    public void onReceivedRequest(RequestMessage message) {
        normalState.handleRequestMessage(message);
    }

    public void onReceivedPrepare(PrepareMessage message) {
        normalState.handlePrepareMessage(message);
    }

    public void onReceivedPrepareOk(PrepareOkMessage message) {
        normalState.handlePrepareOkMessage(message);
    }

    public void onReceivedCommit(CommitMessage message) {
        normalState.handleCommitMessage(message);
    }

    public void onReceivedReply(ReplyMessage reply) {
        wrapper.forwardReply(reply);
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
        recoveryState.handleRecoveryMessage(message);
    }

    public void onReceivedRecoveryResponse(RecoveryResponseMessage message) {
        recoveryState.handleRecoveryResponseMessage(message);
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

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
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
