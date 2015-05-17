package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.message.MessagePrepare;
import com.khovanskiy.dkvstorage.vr.message.MessageRequest;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Victor Khovanskiy
 */
public class Replica {
    public Replica[] configuration;
    public int replicaNumber;
    public int viewNumber;
    public ReplicaStatus status;
    public int opNumber;
    public Map<Integer, LogEntry> log = new HashMap<>();
    public int commitNumber;
    public Map<Integer, ClientEntry> clientTable = new HashMap<>();

    public Replica(int replicaNumber) {
        this.replicaNumber = replicaNumber;
    }

    public synchronized void onRecieved(Message message) {
        if (message instanceof MessageRequest) {
            MessageRequest request = (MessageRequest) message;
            ClientEntry entry = clientTable.get(request.getClientId());

            // If the request-number s isn’t bigger than the information in the table
            if (request.getRequestNumber() < entry.getRequest().getRequestNumber()) {
                // drops
                return;
            }
            // if the request is the most recent one from this client and it has already been executed
            if (request.getRequestNumber() == entry.getRequest().getRequestNumber() && entry.hasExecuted()) {
                // re-send the response
            }
            // advances op-number
            opNumber++;

            // adds the request to the end of the log
            // log.add(request);

            // updates the information for this client in the client-table to contain the new request number
            entry.setRequest(request);

            // sends a <PREPARE v, m, n, k> message to the other replicas
            Message prepare = new MessagePrepare(viewNumber, request, opNumber, commitNumber);
            for (Replica replica : configuration) {
                if (replicaNumber != replica.replicaNumber) {
                    replica.sendMessage(prepare);
                }
            }
        } else if (message instanceof MessagePrepare) {
            MessagePrepare prepare = (MessagePrepare) message;

            //increments its op-number
            opNumber++;

            // adds the request to the end of its log
            // log.add(request);

            ClientEntry entry = clientTable.get(prepare.getRequest().getClientId());
            entry.setRequest(prepare.getRequest());

        }
    }

    public void sendMessage(Message message) {

    }

    public boolean isPrimary() {
        return false;
    }
}
