package com.khovanskiy.dkvstorage.vr.message;

/**
 * @author Victor Khovanskiy
 */
public class MessageReply extends Message {

    private String reply;

    public MessageReply(String reply) {
        this.reply = reply;
    }

    @Override
    public String toString() {
        return reply;
    }
}
