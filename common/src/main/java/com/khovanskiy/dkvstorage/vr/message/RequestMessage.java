package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.khovanskiy.dkvstorage.vr.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class RequestMessage extends Message {

    public static final String TYPE = "request";
    public static final String OPERATION = "operation";
    public static final String CLIENT_ID = "clientId";
    public static final String REQUEST_NUMBER = "requestNumber";

    private final Operation operation;
    private final int clientId;
    private final int requestNumber;

    public RequestMessage(JsonObject jsonObject) {
        this.operation = Operation.decode(jsonObject.getString(OPERATION));
        this.clientId = jsonObject.getInt(CLIENT_ID);
        this.requestNumber = jsonObject.getJsonNumber(REQUEST_NUMBER).intValue();
    }

    public RequestMessage(Operation operation, int clientId, int requestNumber) {
        this.operation = operation;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
    }

    /**
     * Gets operation assigned to this request message
     *
     * @return
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * Gets client id of operation sender
     *
     * @return
     */
    public int getClientId() {
        return clientId;
    }

    /**
     * Gets request number of operation given by client
     *
     * @return
     */
    public int getRequestNumber() {
        return requestNumber;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedRequest(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(OPERATION, Operation.encode(operation))
                .add(CLIENT_ID, clientId)
                .add(REQUEST_NUMBER, requestNumber)
                .build();
    }


}
