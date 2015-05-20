package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.MessageRequest;

/**
 * @author Victor Khovanskiy
 */
public class LogEntry {

    public MessageRequest getRequest() {
        return request;
    }

    private MessageRequest request;

    public LogEntry(MessageRequest request) {
        this.request = request;
    }
}
