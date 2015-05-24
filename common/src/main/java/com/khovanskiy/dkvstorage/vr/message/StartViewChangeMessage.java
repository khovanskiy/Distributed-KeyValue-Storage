package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class StartViewChangeMessage extends Message {
    public static final String TYPE = "startViewChange";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String REPLICA_NUMBER = "replicaNumber";

    private final long viewNumber;
    private final int replicaNumber;

    public StartViewChangeMessage(JsonObject jsonObject) {
        this.viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).longValue();
        this.replicaNumber = jsonObject.getJsonNumber(REPLICA_NUMBER).intValue();
    }

    public StartViewChangeMessage(long viewNumber, int replicaNumber) {
        this.viewNumber = viewNumber;
        this.replicaNumber = replicaNumber;
    }

    public long getViewNumber() {
        return viewNumber;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(VIEW_NUMBER, viewNumber)
                .add(REPLICA_NUMBER, replicaNumber)
                .build();
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedStartViewChange(this);
    }
}
