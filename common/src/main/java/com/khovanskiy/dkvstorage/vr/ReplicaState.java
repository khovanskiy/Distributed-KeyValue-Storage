package com.khovanskiy.dkvstorage.vr;

import java.net.InetSocketAddress;

/**
 * @author Victor Khovanskiy
 */
public class ReplicaState {
    public InetSocketAddress[] configuration;
    public int replicaNumber;
    public int viewNumber;
    public ReplicaStatus status;
    public int opNumber;
    //public log
    public int commitNumber;
    public int clientTable;
}
