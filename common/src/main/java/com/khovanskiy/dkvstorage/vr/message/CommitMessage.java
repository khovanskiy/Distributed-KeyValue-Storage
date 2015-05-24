package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class CommitMessage extends Message {

    public static final String TYPE = "commit";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String COMMIT_NUMBER = "commitNumber";

    private final long viewNumber;
    private final long commitNumber;

    public CommitMessage(JsonObject jSonObject) {
        this.viewNumber = jSonObject.getJsonNumber(VIEW_NUMBER).longValue();
        this.commitNumber = jSonObject.getJsonNumber(COMMIT_NUMBER).longValue();
    }

    public CommitMessage(long viewNumber, long commitNumber) {
        this.viewNumber = viewNumber;
        this.commitNumber = commitNumber;
    }

    public long getViewNumber() {
        return viewNumber;
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedCommit(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(VIEW_NUMBER, viewNumber)
                .add(COMMIT_NUMBER, commitNumber)
                .build();
    }
}
