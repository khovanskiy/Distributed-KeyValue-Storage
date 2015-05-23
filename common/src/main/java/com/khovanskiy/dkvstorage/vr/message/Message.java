package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

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

    public static String encode(Message message) {
        return Json.createObjectBuilder()
                .add(Message.MESSAGE_TYPE, message.getMessageType())
                .add(Message.MESSAGE_CONTENT, message.encode())
                .build().toString();
    }

    public static Message decode(String json) {
        JsonReader jsonReader = Json.createReader(new StringReader(json));
        JsonObject jsonObject = jsonReader.readObject();

        JsonString type = jsonObject.getJsonString(Message.MESSAGE_TYPE);
        JsonObject content = jsonObject.getJsonObject(Message.MESSAGE_CONTENT);

        switch (type.getString()) {
            case RequestMessage.TYPE:
                return new RequestMessage(content);
            case PrepareMessage.TYPE:
                return new PrepareMessage(content);
            case PrepareOkMessage.TYPE:
                return new PrepareOkMessage(content);
            /*case CommitMessage.ID:
                return new CommitMessage(messageBody);
            case StartViewChangeMessage.ID:
                return new StartViewChangeMessage(messageBody);*/
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
