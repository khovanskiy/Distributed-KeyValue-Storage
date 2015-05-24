package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.RecoveryMessage;
import com.khovanskiy.dkvstorage.vr.message.RecoveryResponseMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of recovery protocol
 *
 * @author Victor Khovanskiy
 */
public class RecoveryState {

    private final Replica replica;
    private final List<RecoveryResponseMessage> recoveryResponseMessages = new ArrayList<>();
    private RecoveryResponseMessage recoveryResponseFromPrimary;
    private long timestamp;

    public RecoveryState(Replica replica) {
        this.replica = replica;
    }

    public void startRecovery() {
        if (replica.getStatus() != ReplicaStatus.RECOVERING) {
            replica.setStatus(ReplicaStatus.RECOVERING);

            // The recovering replica i sends a [RECOVERY i, x] message to all other replicas, where x is a nonce
            timestamp = Utils.timeStamp();
            replica.getWrapper().sendToOtherReplicas(new RecoveryMessage(replica.getReplicaNumber(), timestamp));
        }
    }

    public void processRecoveryMessage(RecoveryMessage message) {
        // replica replies to a RECOVERY message only when its status is normal
        if (replica.getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        // In this case the replica sends a [RECOVERY_RESPONSE v, x, l, n, k, j] message to the recovering replica
        if (replica.isPrimary()) {
            //  If replica is the primary of its view, l is its log, n is its op-number, and k is the commit-number
            replica.getWrapper().sendToReplica(message.getReplicaNumber(), new RecoveryResponseMessage(replica.getViewNumber(), replica.getOperationNumber(), replica.getCommitNumber(), message.getTimestamp(), replica.getLog()));
        } else {
            // otherwise these values are nil
            replica.getWrapper().sendToReplica(message.getReplicaNumber(), new RecoveryResponseMessage(0, 0, 0, message.getTimestamp(), null));
        }
    }

    public void processRecoveryResponseMessage(RecoveryResponseMessage message) {
        if (replica.getStatus() != ReplicaStatus.RECOVERING) {
            return;
        }

        // all containing the nonce it sent in its RECOVERY message
        if (message.getTimestamp() != timestamp) {
            return;
        }

        recoveryResponseMessages.add(message);

        // including one from the primary
        if (message.getLog() != null) {
            // of the latest view it learns of in these messages
            if (recoveryResponseFromPrimary == null || recoveryResponseFromPrimary.getViewNumber() < message.getViewNumber()) {
                recoveryResponseFromPrimary = message;
            }
        }

        // The recovering replica waits to receive at least f + 1 RECOVERY_RESPONSE messages from different replicas,
        if (recoveryResponseFromPrimary != null && recoveryResponseMessages.size() >= replica.getConfiguration().size() / 2 + 1) {
            // Then it updates its state using the information from the primary
            replica.setViewNumber(recoveryResponseFromPrimary.getViewNumber());
            replica.setOperationNumber(recoveryResponseFromPrimary.getOperationNumber());
            replica.setCommitNumber(recoveryResponseFromPrimary.getCommitNumber());
            replica.setLog(recoveryResponseFromPrimary.getLog());

            // changes its status to normal
            replica.setStatus(ReplicaStatus.NORMAL);

            // and the recovery protocol is complete.
            Utils.log(replica.getReplicaNumber(), "Recovery protocol is complete");
        }
    }
}
