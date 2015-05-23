package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.Replica;

import javax.json.Json;
import javax.json.JsonObject;

/**
 * @author Victor Khovanskiy
 */
public class PrepareOkMessage extends Message {

    public static final String TYPE = "prepareOk";
    public static final String VIEW_NUMBER = "ViewNumber";
    public static final String OPERATION_NUMBER = "OperationNumber";
    public static final String NODE_NUMBER = "NodeNumber";

    public int getViewNumber() {
        return viewNumber;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getBackupNumber() {
        return backupNumber;
    }

    private int viewNumber;
    private int opNumber;
    private int backupNumber;

    public PrepareOkMessage(JsonObject jsonObject) {
        this.viewNumber = jsonObject.getJsonNumber(VIEW_NUMBER).intValue();
        this.opNumber = jsonObject.getJsonNumber(OPERATION_NUMBER).intValue();
        this.backupNumber = jsonObject.getInt(NODE_NUMBER);
    }

    public PrepareOkMessage(int viewNumber, int opNumber, int backupNumber) {
        this.viewNumber = viewNumber;
        this.opNumber = opNumber;
        this.backupNumber = backupNumber;
    }

    @Override
    public void delegateProcessing(Replica replica) {
        replica.onReceivedPrepareOk(this);
    }

    @Override
    public String getMessageType() {
        return TYPE;
    }

    @Override
    protected JsonObject encode() {
        return Json.createObjectBuilder()
                .add(VIEW_NUMBER, viewNumber)
                .add(OPERATION_NUMBER, opNumber)
                .add(NODE_NUMBER, backupNumber)
                .build();
    }
}
