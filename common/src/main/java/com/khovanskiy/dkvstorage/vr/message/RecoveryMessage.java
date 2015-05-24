package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class RecoveryMessage extends Message {

    public static final String TYPE = "recovery";
    public static final String REPLICA_NUMBER = "replicaNumber";
    public static final String TIMESTAMP = "timestamp";

    private final int replicaNumber;
    private final long timestamp;

    public RecoveryMessage(int replicaNumber, long timestamp) {
        this.replicaNumber = replicaNumber;
        this.timestamp = timestamp;
    }

    public RecoveryMessage(JsonObject jsonObject) {
        replicaNumber = jsonObject.getJsonNumber(REPLICA_NUMBER).intValue();
        timestamp = jsonObject.getJsonNumber(TIMESTAMP).longValue();
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedRecovery(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(REPLICA_NUMBER, replicaNumber)
                .add(TIMESTAMP, timestamp)
                .build();
    }
}
