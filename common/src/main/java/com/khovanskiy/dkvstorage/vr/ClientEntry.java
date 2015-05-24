package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;

/**
 * @author Victor Khovanskiy
 */
public class ClientEntry {
    private String result;
    private boolean processing;
    private int clientId;
    private long requestNumber;

    public ClientEntry(int clientId) {
        this.clientId = clientId;
    }

    public long getRequestNumber() {
        return requestNumber;
    }

    public void setRequestNumber(long requestNumber) {
        this.requestNumber = requestNumber;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }


    public boolean isProcessing() {
        return processing;
    }

    public void setProcessing(boolean processing) {
        this.processing = processing;
    }
}
