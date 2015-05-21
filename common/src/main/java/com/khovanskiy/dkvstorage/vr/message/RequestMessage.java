package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.khovanskiy.dkvstorage.vr.Replica;
import org.json.simple.JSONObject;

/**
 * @author Victor Khovanskiy
 */
public class RequestMessage extends Message {
    private Operation operation;
    private int clientId;
    private int requestNumber;

    public RequestMessage(Operation operation, int clientId, int requestNumber) {
        this.operation = operation;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
    }

    public Operation getOperation() {
        return operation;
    }

    public int getClientId() {
        return clientId;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedRequest(this);
    }

    @Override
    public String toString() {
        return operation + " " + clientId + " " + requestNumber;
    }
}