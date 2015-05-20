package com.khovanskiy.dkvstorage.vr.operation;

import com.khovanskiy.dkvstorage.vr.Replica;
import com.khovanskiy.dkvstorage.vr.message.IdentificationMessage;
import com.khovanskiy.dkvstorage.vr.message.MessageReply;
import org.json.simple.parser.JSONParser;

/**
 * @author Victor Khovanskiy
 */
public abstract class Operation {
    public abstract MessageReply delegateUpCall(Replica replica);
}
