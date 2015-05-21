package com.khovanskiy.dkvstorage.vr.message;

/**
 * @author Victor Khovanskiy
 */
public class PrepareOkMessage extends Message {

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

    public PrepareOkMessage(int viewNumber, int opNumber, int backupNumber) {
        this.viewNumber = viewNumber;
        this.opNumber = opNumber;
        this.backupNumber = backupNumber;
    }
}
