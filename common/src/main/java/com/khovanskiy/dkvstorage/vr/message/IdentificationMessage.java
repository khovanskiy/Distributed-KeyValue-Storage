package com.khovanskiy.dkvstorage.vr.message;

import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 *
 * @deprecated
 */
public class IdentificationMessage extends Message {

    private int id;

    public IdentificationMessage(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public String getMessageType() {
        return null;
    }

    @Override
    protected JsonObject encode() {
        return null;
    }

    @Override
    public String toString() {
        return "node " + id;
    }
}
