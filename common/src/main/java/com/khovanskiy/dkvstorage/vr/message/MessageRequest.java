package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Operation;

/**
 * @author Victor Khovanskiy
 */
public class MessageRequest extends Message {
    private Operation operation;
    private int clientId;
    private int requestNumber;

    public MessageRequest(Operation operation, int clientId, int requestNumber) {
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
}
