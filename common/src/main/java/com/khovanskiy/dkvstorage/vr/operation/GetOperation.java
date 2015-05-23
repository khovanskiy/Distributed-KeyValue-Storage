package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class GetOperation extends Operation {

    public static final String TYPE = "get";
    public static final String KEY = "key";

    private final String key;

    public GetOperation(JsonObject jsonObject) {
        this.key = jsonObject.getString(KEY);
    }

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
    public String delegateUpCall(Replica replica) {
        String value = replica.storage.get(key);
        if (value != null) {
            return "VALUE " + key + " " + value;
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
