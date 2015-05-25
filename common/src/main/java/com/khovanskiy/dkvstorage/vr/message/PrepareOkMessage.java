package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.replica.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class PrepareOkMessage extends Message {

    public static final String TYPE = "prepareOk";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String OPERATION_NUMBER = "operationNumber";
    public static final String REPLICA_NUMBER = "replica";

    public long getViewNumber() {
        return viewNumber;
    }

    public long getOperationNumber() {
        return operationNumber;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    private long viewNumber;
    private long operationNumber;
    private int replicaNumber;

    public PrepareOkMessage(JsonObject jsonObject) {
        this.viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).longValue();
        this.operationNumber = jsonObject.getJsonNumber(OPERATION_NUMBER).longValue();
        this.replicaNumber = jsonObject.getInt(REPLICA_NUMBER);
    }

    public PrepareOkMessage(long viewNumber, long operationNumber, int replicaNumber) {
        this.viewNumber = viewNumber;
        this.operationNumber = operationNumber;
        this.replicaNumber = replicaNumber;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedPrepareOk(this);
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
                .add(REPLICA_NUMBER, replicaNumber)
                .build();
    }
}
