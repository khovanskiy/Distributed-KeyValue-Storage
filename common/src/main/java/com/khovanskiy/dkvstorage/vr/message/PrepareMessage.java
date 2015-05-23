package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class PrepareMessage extends Message {

    public static final String TYPE = "prepare";
    public static final String REQUEST_MESSAGE = "request";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String OPERATION_NUMBER = "opNumber";
    public static final String COMMIT_NUMBER = "commitNumber";

    private int viewNumber;
    private RequestMessage request;
    private int opNumber;
    private int commitNumber;

    public PrepareMessage(JsonObject jsonObject) {
        this.request = (RequestMessage) Message.decode(jsonObject.getString(REQUEST_MESSAGE));
        this.viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).intValue();
        this.opNumber = jsonObject.getJsonNumber(OPERATION_NUMBER).intValue();
        this.commitNumber = jsonObject.getJsonNumber(COMMIT_NUMBER).intValue();
    }

    public PrepareMessage(RequestMessage message, int viewNumber, int opNumber, int commitNumber) {
        this.request = message;
        this.viewNumber = viewNumber;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }

    public RequestMessage getRequest() {
        return request;
    }

    public int getViewNumber() {
        return viewNumber;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedPrepare(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(REQUEST_MESSAGE, Message.encode(request))
                .add(VIEW_NUMBER, viewNumber)
                .add(OPERATION_NUMBER, opNumber)
                .add(COMMIT_NUMBER, commitNumber)
                .build();
    }

    @Override
    public String toString() {
        return "prepare " + viewNumber + " " + request + " " + opNumber + " " + commitNumber;
    }
}
