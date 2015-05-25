package com.khovanskiy.dkvstorage.vr.replica;

import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;

import javax.json.*;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Victor Khovanskiy
 */
public class ReplicaLog extends HashMap<Long, RequestMessage> {

    public static ReplicaLog decode(String json) {
        JsonReader jsonReader = Json.createReader(new StringReader(json));
        JsonObject jsonObject = jsonReader.readObject();

        ReplicaLog log = new ReplicaLog();

        for (Map.Entry<String, JsonValue> entry : jsonObject.entrySet()) {
            log.put(Long.parseLong(entry.getKey()), (RequestMessage) Message.decode((JsonObject) entry.getValue()));
        }

        return log;
    }

    public static JsonObject encode(ReplicaLog log) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        for (Map.Entry<Long, RequestMessage> entry : log.entrySet()) {
            builder.add(entry.getKey().toString(), Message.encode(entry.getValue()));
        }
        return builder.build();
    }
}
