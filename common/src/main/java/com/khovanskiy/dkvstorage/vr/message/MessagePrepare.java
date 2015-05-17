package com.khovanskiy.dkvstorage.vr.message;

/**
 * @author Victor Khovanskiy
 */
public class MessagePrepare extends Message {
    private int viewNumber;
    private MessageRequest request;
    private int opNumber;
    private int commitNumber;

    public MessagePrepare(int viewNumber, MessageRequest message, int opNumber, int commitNumber) {
        this.viewNumber = viewNumber;
        this.request = message;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }


    public MessageRequest getRequest() {
        return request;
    }
}
