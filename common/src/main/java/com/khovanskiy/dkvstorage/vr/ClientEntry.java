package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.MessageReply;
import com.khovanskiy.dkvstorage.vr.message.MessageRequest;

/**
 * @author Victor Khovanskiy
 */
public class ClientEntry {
    private MessageRequest request;
    private MessageReply reply;

    public ClientEntry(MessageRequest request) {
        this.request = request;
    }

    public MessageReply getReply() {
        return reply;
    }

    public void setReply(MessageReply reply) {
        this.reply = reply;
    }

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
