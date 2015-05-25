package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.replica.Replica;
import com.khovanskiy.dkvstorage.vr.replica.ReplicaLog;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * @author Victor Khovanskiy
 */
public class RecoveryResponseMessage extends Message {

    public static final String TYPE = "recoveryResponse";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String OPERATION_NUMBER = "operationNumber";
    public static final String COMMIT_NUMBER = "commitNumber";
    public static final String TIMESTAMP = "timestamp";
    public static final String LOG = "log";

    private final long viewNumber;
    private final long operationNumber;
    private final long commitNumber;
    private final long timestamp;
    private final ReplicaLog log;

    public RecoveryResponseMessage(long viewNumber, long operationNumber, long commitNumber,
                                   long timestamp, ReplicaLog log) {
        this.viewNumber = viewNumber;
        this.operationNumber = operationNumber;
        this.commitNumber = commitNumber;
        this.timestamp = timestamp;
        this.log = log;
    }

    public RecoveryResponseMessage(JsonObject jsonObject) {
        viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).longValue();
        operationNumber = jsonObject.getJsonNumber(OPERATION_NUMBER).longValue();
        commitNumber = jsonObject.getJsonNumber(COMMIT_NUMBER).longValue();
        timestamp = jsonObject.getJsonNumber(TIMESTAMP).longValue();
        if (!jsonObject.isNull(LOG)) {
            log = ReplicaLog.decode(jsonObject.getJsonObject(LOG).toString());
        } else {
            log = null;
        }
    }

    public long getViewNumber() {
        return viewNumber;
    }

    public long getOperationNumber() {
        return operationNumber;
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ReplicaLog getLog() {
        return log;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedRecoveryResponse(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        JsonObjectBuilder builder = Json.createObjectBuilder()
                .add(VIEW_NUMBER, viewNumber)
                .add(OPERATION_NUMBER, operationNumber)
                .add(COMMIT_NUMBER, commitNumber)
                .add(TIMESTAMP, timestamp);
        if (log != null) {
            builder.add(LOG, ReplicaLog.encode(log));
        } else {
            builder.addNull(LOG);
        }
        return builder.build();
    }
}
