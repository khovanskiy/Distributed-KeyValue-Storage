package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.MessageReply;

/**
 * @author Victor Khovanskiy
 */
public class GetOperation extends Operation {
    private String key;
    public GetOperation(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "get " + key;
    }

    @Override
    public MessageReply delegateUpCall(Replica replica) {
        String value = replica.storage.get(key);
        if (value != null) {
            return new MessageReply("VALUE " + key + " " + value);
        }
        return new MessageReply("NOT_FOUND");
    }
}
