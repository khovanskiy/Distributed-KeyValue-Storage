package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.MessageReply;

/**
 * @author Victor Khovanskiy
 */
public class SetOperation extends Operation {
    private String key;
    private String value;

    public SetOperation(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "set " + key + " " + value;
    }

    @Override
    public MessageReply delegateUpCall(Replica replica) {
        replica.storage.put(key, value);
        return new MessageReply("STORED");
    }
}
