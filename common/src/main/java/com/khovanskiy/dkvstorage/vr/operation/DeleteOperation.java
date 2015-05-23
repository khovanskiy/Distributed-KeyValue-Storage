package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class DeleteOperation extends Operation {

    public static final String TYPE = "delete";
    public static final String KEY = "key";

    private final String key;

    public DeleteOperation(JsonObject jsonObject) {
        this.key = jsonObject.getString(KEY);
    }

    public DeleteOperation(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "delete " + key;
    }

    @Override
    public String delegateUpCall(Replica replica) {
        if (replica.storage.containsKey(key)) {
            replica.storage.remove(key);
            return "DELETED";
        }
        return "NOT_FOUND";
    }

    @Override
    public String getOperationType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(KEY, key)
                .build();
    }
}
