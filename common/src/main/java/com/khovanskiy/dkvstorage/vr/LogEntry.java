package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.RequestMessage;

/**
 * @author Victor Khovanskiy
 */
public class LogEntry {

    public RequestMessage getRequest() {
        return request;
    }

    private RequestMessage request;

    public LogEntry(RequestMessage request) {
        this.request = request;
    }
}
