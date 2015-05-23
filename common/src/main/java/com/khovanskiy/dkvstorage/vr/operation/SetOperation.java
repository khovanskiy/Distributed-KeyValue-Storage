package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class SetOperation extends Operation {
    public static final String TYPE = "set";
    public static final String KEY = "key";
    public static final String VALUE = "value";

    private final String key;
    private final String value;

    public SetOperation(JsonObject jsonObject) {
        this.key = jsonObject.getString(KEY);
        this.value = jsonObject.getString(VALUE);
    }

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
    public String delegateUpCall(Replica replica) {
        replica.storage.put(key, value);
        return "STORED";
    }

    @Override
    public String getOperationType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(KEY, key)
                .add(VALUE, value)
                .build();
    }
}
