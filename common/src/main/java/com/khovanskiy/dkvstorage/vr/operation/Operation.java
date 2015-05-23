package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.ReplyMessage;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import java.io.StringReader;

/**
 * @author Victor Khovanskiy
 */
public abstract class Operation {
    public static final String OPERATION_TYPE = "type";

    public static final String OPERATION_CONTENT = "content";

    public static String encode(Operation operation) {
        return Json.createObjectBuilder()
                .add(Operation.OPERATION_TYPE, operation.getOperationType())
                .add(Operation.OPERATION_CONTENT, operation.encode())
                .build().toString();
    }

    public static Operation decode(String json) {
        JsonReader jsonReader = Json.createReader(new StringReader(json));
        JsonObject jsonObject = jsonReader.readObject();

        JsonString type = jsonObject.getJsonString(Operation.OPERATION_TYPE);
        JsonObject content = jsonObject.getJsonObject(Operation.OPERATION_CONTENT);

        switch (type.getString()) {
            case GetOperation.TYPE:
                return new GetOperation(content);
            case SetOperation.TYPE:
                return new SetOperation(content);
            case DeleteOperation.TYPE:
                return new DeleteOperation(content);
        }

        throw new IllegalArgumentException("Unknown operation type: \"" + type + "\"");
    }

    public abstract String delegateUpCall(Replica replica);

    public abstract String getOperationType();

    protected abstract JsonObject encode();
}
