package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

/**
 * @author Victor Khovanskiy
 */
public class PrepareMessage extends Message {
    private int viewNumber;
    private RequestMessage request;
    private int opNumber;
    private int commitNumber;

    public PrepareMessage(int viewNumber, RequestMessage message, int opNumber, int commitNumber) {
        this.viewNumber = viewNumber;
        this.request = message;
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
    public String toString() {
        return "prepare " + viewNumber + " " + request + " " + opNumber + " " + commitNumber;
    }
}
