package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.replica.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class ReplyMessage extends Message {

    public static final String TYPE = "reply";
    public static final String VIEW_NUMBER = "viewNumber";
    public static final String REQUEST_NUMBER = "requestNumber";
    public static final String OPERATION_RESULT = "result";
    private final long viewNumber;
    private final long requestNumber;

    public String getResult() {
        return result;
    }

    private final String result;

    public ReplyMessage(JsonObject jsonObject) {
        viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).intValue();
        requestNumber = jsonObject.getInt(REQUEST_NUMBER);
        result = jsonObject.getString(OPERATION_RESULT);
    }

    public ReplyMessage(long viewNumber, long requestNumber, String result) {
        this.viewNumber = viewNumber;
        this.requestNumber = requestNumber;
        this.result = result;
    }

    public long getRequestNumber() {
        return requestNumber;
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(VIEW_NUMBER, viewNumber)
                .add(REQUEST_NUMBER, requestNumber)
                .add(OPERATION_RESULT, result)
                .build();
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedReply(this);
    }
}
