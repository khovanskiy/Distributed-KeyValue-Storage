package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

/**
 * @author Victor Khovanskiy
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
    public String toString() {
        return "node " + id;
    }
}
