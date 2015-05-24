package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.ReplicaLog;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class StartViewMessage extends Message {

    public static final String TYPE = "startView";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String OPERATION_NUMBER = "operationNumber";
    public static final String COMMIT_NUMBER = "commitNumber";
    public static final String LOG = "log";

    private final long viewNumber;
    private final long operationNumber;
    private final long commitNumber;
    private final ReplicaLog log;

    public StartViewMessage(JsonObject jsonObject) {
        viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).longValue();
        operationNumber = jsonObject.getJsonNumber(OPERATION_NUMBER).longValue();
        commitNumber = jsonObject.getJsonNumber(COMMIT_NUMBER).longValue();
        log = ReplicaLog.decode(jsonObject.get(LOG).toString());
    }

    public StartViewMessage(long viewNumber, long operationNumber, long commitNumber, ReplicaLog log) {
        this.viewNumber = viewNumber;
        this.operationNumber = operationNumber;
        this.commitNumber = commitNumber;
        this.log = log;
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

    public ReplicaLog getLog() {
        return log;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedStartView(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(VIEW_NUMBER, viewNumber)
                .add(OPERATION_NUMBER, operationNumber)
                .add(COMMIT_NUMBER, commitNumber)
                .add(LOG, ReplicaLog.encode(log))
                .build();
    }
}
