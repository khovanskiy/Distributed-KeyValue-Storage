package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.MessageReply;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;

/**
 * @author Victor Khovanskiy
 */
public class ClientEntry {
    private RequestMessage request;
    private MessageReply reply;
    private boolean processing;

    public ClientEntry(RequestMessage request) {
        this.request = request;
    }

    public MessageReply getReply() {
        return reply;
    }

    public void setReply(MessageReply reply) {
        this.reply = reply;
    }


    public RequestMessage getRequest() {
        return request;
    }

    public void setRequest(RequestMessage request) {
        this.request = request;
    }

    public boolean isProcessing() {
        return processing;
    }

    public void setProcessing(boolean processing) {
        this.processing = processing;
    }
}
