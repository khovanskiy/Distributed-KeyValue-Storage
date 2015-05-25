package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.replica.Replica;
import com.khovanskiy.dkvstorage.vr.replica.ReplicaLog;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class DoViewChangeMessage extends Message {

    public static final String TYPE = "doViewChange";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String LAST_NORMAL_VIEW_NUMBER = "LastNormalViewNumber";
    public static final String OPERATION_NUMBER = "opNumber";
    public static final String COMMIT_NUMBER = "commitNumber";
    public static final String REPLICA_NUMBER = "replicaNumber";
    public static final String LOG = "log";

    private final long viewNumber;
    private final long lastNormalViewNumber;
    private final long operationNumber;
    private final long commitNumber;
    private final int replicaNumber;
    private final ReplicaLog log;

    public DoViewChangeMessage(JsonObject jsonObject) {
        viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).intValue();
        lastNormalViewNumber = jsonObject.getJsonNumber(LAST_NORMAL_VIEW_NUMBER).intValue();
        operationNumber = jsonObject.getJsonNumber(OPERATION_NUMBER).intValue();
        commitNumber = jsonObject.getJsonNumber(COMMIT_NUMBER).intValue();
        replicaNumber = jsonObject.getInt(REPLICA_NUMBER);
        log = ReplicaLog.decode(jsonObject.get(LOG).toString());
    }

    public DoViewChangeMessage(long viewNumber, long lastNormalViewNumber, long operationNumber,
                               long commitNumber, int replicaNumber, ReplicaLog log) {
        this.viewNumber = viewNumber;
        this.lastNormalViewNumber = lastNormalViewNumber;
        this.operationNumber = operationNumber;
        this.commitNumber = commitNumber;
        this.replicaNumber = replicaNumber;
        this.log = log;
    }

    public long getViewNumber() {
        return viewNumber;
    }

    public long getLastNormalViewNumber() {
        return lastNormalViewNumber;
    }

    public long getOperationNumber() {
        return operationNumber;
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public ReplicaLog getLog() {
        return log;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedDoViewChange(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(VIEW_NUMBER, viewNumber)
                .add(LAST_NORMAL_VIEW_NUMBER, lastNormalViewNumber)
                .add(OPERATION_NUMBER, operationNumber)
                .add(COMMIT_NUMBER, commitNumber)
                .add(REPLICA_NUMBER, replicaNumber)
                .add(LOG, ReplicaLog.encode(log))
                .build();
    }
}
