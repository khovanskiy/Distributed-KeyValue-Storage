package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.replica.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class PrepareMessage extends Message {

    public static final String TYPE = "prepare";
    public static final String REQUEST_MESSAGE = "request";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String OPERATION_NUMBER = "operationNumber";
    public static final String COMMIT_NUMBER = "commitNumber";

    private long viewNumber;
    private RequestMessage request;
    private long operationNumber;
    private long commitNumber;

    public PrepareMessage(JsonObject jsonObject) {
        this.request = (RequestMessage) Message.decode(jsonObject.getJsonObject(REQUEST_MESSAGE));
        this.viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).longValue();
        this.operationNumber = jsonObject.getJsonNumber(OPERATION_NUMBER).longValue();
        this.commitNumber = jsonObject.getJsonNumber(COMMIT_NUMBER).longValue();
    }

    public PrepareMessage(RequestMessage message, long viewNumber, long operationNumber, long commitNumber) {
        this.request = message;
        this.viewNumber = viewNumber;
        this.operationNumber = operationNumber;
        this.commitNumber = commitNumber;
    }

    public RequestMessage getRequest() {
        return request;
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
                .add(OPERATION_NUMBER, operationNumber)
                .add(COMMIT_NUMBER, commitNumber)
                .build();
    }

    @Override
    public String toString() {
        return "prepare " + viewNumber + " " + request + " " + operationNumber + " " + commitNumber;
    }
}
