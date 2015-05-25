package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.replica.Replica;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import java.io.StringReader;

/**
 * @author Victor Khovanskiy
 */
public abstract class Message {

    private static final String MESSAGE_TYPE = "type";
    private static final String MESSAGE_CONTENT = "content";

    public static JsonObject encode(Message message) {
        return Json.createObjectBuilder()
                .add(Message.MESSAGE_TYPE, message.getMessageType())
                .add(Message.MESSAGE_CONTENT, message.encode())
                .build();
    }

    public static Message decode(String jsonString) {
        JsonReader jsonReader = Json.createReader(new StringReader(jsonString));
        JsonObject jsonObject = jsonReader.readObject();
        return decode(jsonObject);
    }

    public static Message decode(JsonObject jsonObject) {
        JsonString type = jsonObject.getJsonString(Message.MESSAGE_TYPE);
        JsonObject content = jsonObject.getJsonObject(Message.MESSAGE_CONTENT);

        switch (type.getString()) {
            case RequestMessage.TYPE:
                return new RequestMessage(content);
            case PrepareMessage.TYPE:
                return new PrepareMessage(content);
            case PrepareOkMessage.TYPE:
                return new PrepareOkMessage(content);
            case CommitMessage.TYPE:
                return new CommitMessage(content);
            case StartViewChangeMessage.TYPE:
                return new StartViewChangeMessage(content);
            case DoViewChangeMessage.TYPE:
                return new DoViewChangeMessage(content);
            case StartViewMessage.TYPE:
                return new StartViewMessage(content);
            case RecoveryMessage.TYPE:
                return new RecoveryMessage(content);
            case RecoveryResponseMessage.TYPE:
                return new RecoveryResponseMessage(content);
            case ReplyMessage.TYPE:
                return new ReplyMessage(content);
        }
        throw new IllegalArgumentException("Unknown message type: \"" + type + "\"");
    }

    public void delegateProcessing(Replica replica) {

    }

    public abstract String getMessageType();

    protected abstract JsonObject encode();

    @Override
    public String toString() {
        return getMessageType() + ":" + encode().toString();
    }
}
