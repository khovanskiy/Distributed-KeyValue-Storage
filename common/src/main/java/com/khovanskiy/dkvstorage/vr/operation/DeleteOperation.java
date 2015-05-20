package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.MessageReply;

/**
 * @author Victor Khovanskiy
 */
public class DeleteOperation extends Operation {
    private String key;

    public DeleteOperation(String key) {
        super();
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "delete " + key;
    }

    @Override
    public MessageReply delegateUpCall(Replica replica) {
        if (replica.storage.containsKey(key)) {
            replica.storage.remove(key);
            return new MessageReply("DELETED");
        }
        return new MessageReply("NOT_FOUND");
    }
}
