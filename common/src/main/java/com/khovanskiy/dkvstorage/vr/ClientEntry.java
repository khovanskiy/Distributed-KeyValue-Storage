package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.MessageRequest;

/**
 * @author Victor Khovanskiy
 */
public class ClientEntry {
    private MessageRequest request;

    public boolean hasExecuted() {
        return false;
    }

    public MessageRequest getRequest() {
        return request;
    }

    public void setRequest(MessageRequest request) {
        this.request = request;
    }
}
