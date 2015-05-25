package com.khovanskiy.dkvstorage.vr.replica;

import com.khovanskiy.dkvstorage.vr.ClientEntry;
import com.khovanskiy.dkvstorage.vr.ReplicaStatus;
import com.khovanskiy.dkvstorage.vr.Utils;
import com.khovanskiy.dkvstorage.vr.message.*;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.sun.istack.internal.NotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of normal protocol
 *
 * @author Victor Khovanskiy
 */
public class NormalState {
    private final Replica replica;
    private final Map<Long, Map<Integer, Boolean>> ballotRequestTable = new HashMap<>();

    public NormalState(Replica replica) {
        this.replica = replica;
    }

    /**
     * Handles REQUEST message
     *
     * @param message REQUEST message
     */
    public void handleRequestMessage(@NotNull RequestMessage message) {
        // Replicas participate in processing of client requests only when their status is normal
        if (replica.getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        //
        if (!replica.isPrimary()) {
            return;
        }

        ClientEntry entry = replica.getClient(message.getClientId());

        // If the request-number isn’t bigger than the information in the table
        if (message.getRequestNumber() < entry.getRequestNumber()) {
            // drops stale request
            return;
        }

        // if the request is the most recent one from this client and it has already been executed
        if (message.getRequestNumber() == entry.getRequestNumber()) {
            if (entry.isProcessing()) {
                // still processing request
                return;
            } else {
                // re-send the response
                replica.getWrapper().forwardReply(new ReplyMessage(replica.getViewNumber(), message.getRequestNumber(), entry.getResult()));
            }
        }

        // advances op-number
        replica.setOperationNumber(replica.getOperationNumber() + 1);

        // adds the request to the end of the log
        replica.getLog().put(replica.getOperationNumber(), message);

        // updates the information for this client in the client-table to contain the new request number
        entry.setRequestNumber(message.getRequestNumber());
        entry.setProcessing(true);

        // prepare table for ballot by voting backups
        ballotRequestTable.put(replica.getOperationNumber(), new HashMap<>());

        // sends a [PREPARE v, m, n, k] message to the other replicas
        replica.getWrapper().sendToOtherReplicas(new PrepareMessage(message, replica.getViewNumber(), replica.getOperationNumber(), replica.getCommitNumber()));
    }

    /**
     * Handles PREPARE message
     *
     * @param message PREPARE message
     */
    public void handlePrepareMessage(@NotNull PrepareMessage message) {
        // Replicas participate in processing of client requests only when their status is normal
        if (replica.getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        // If the sender is behind, the receiver drops the message
        if (message.getViewNumber() < replica.getViewNumber()) {
            return;
        }

        if (replica.isPrimary()) {
            return;
        }

        if (message.getViewNumber() > replica.getViewNumber()) {
            replica.startRecovery();
            return;
        }

        // Operation with operation-number + 1 or bigger was missing
        if (message.getOperationNumber() > replica.getOperationNumber() + 1) {
            replica.startRecovery();
            return;
        }

        // Ignore out-of-order message
        if (message.getOperationNumber() < replica.getOperationNumber() + 1) {
            return;
        }

        // won’t accept a prepare with op-number n until it has entries for all earlier requests in its log.
        commitUpTo(message.getCommitNumber());

        //increments its op-number
        replica.setOperationNumber(replica.getOperationNumber() + 1);

        // adds the request to the end of its log
        replica.getLog().put(replica.getOperationNumber(), message.getRequest());

        //  updates the client's information in the client-table
        ClientEntry entry = replica.getClient(message.getRequest().getClientId());
        entry.setRequestNumber(message.getRequest().getRequestNumber());

        // and sends a [PREPARE_OK v, n, i] message to the primary to indicate that this operation and all earlier ones have prepared locally.
        replica.getWrapper().sendToPrimary(new PrepareOkMessage(replica.getViewNumber(), replica.getOperationNumber(), replica.getReplicaNumber()));

        // mark that normally the primary informs backups about the commit when it sends the next PREPARE message
        replica.setLastTimestamp(System.currentTimeMillis());
    }

    /**
     * Handles PREPARE_OK message
     *
     * @param message PREPARE_OK message
     */
    public void handlePrepareOkMessage(@NotNull PrepareOkMessage message) {
        // Replicas participate in processing of client requests only when their status is normal
        if (replica.getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        // If the sender is behind, the receiver drops the message
        if (message.getViewNumber() < replica.getViewNumber()) {
            return;
        }

        if (!replica.isPrimary()) {
            return;
        }

        // Ignore committed operations
        if (message.getOperationNumber() <= replica.getCommitNumber()) {
            return;
        }

        // Update table for PrepareOK messages
        Map<Integer, Boolean> ballot = ballotRequestTable.get(message.getOperationNumber());
        if (ballot == null) {
            return;
        }
        ballot.put(message.getReplicaNumber(), true);

        // Commit operations agreed by quorum
        if (message.getOperationNumber() > replica.getCommitNumber() + 1) {
            return;
        }

        boolean changed = false;
        while (replica.getCommitNumber() < replica.getOperationNumber()) {
            ballot = ballotRequestTable.get(replica.getCommitNumber() + 1);

            // The primary waits for f PREPARE_OK messages from different backups;
            if (ballot.size() >= replica.getConfiguration().size() / 2) {
                // at this point it considers the operation (and all earlier ones) to be committed.
                RequestMessage request = replica.getLog().get(replica.getCommitNumber() + 1);

                // Then, after it has executed all earlier operations (those assigned smaller op-numbers), the primary executes the operation by making an up-call
                String result = upCall(request.getOperation());

                // and increments its commit-number.
                replica.setCommitNumber(replica.getCommitNumber() + 1);
                changed = true;

                // The primary also updates the client's entry in the client-table to contain the result.
                ClientEntry entry = replica.getClient(request.getClientId());
                entry.setResult(result);
                entry.setProcessing(false);

                // It sends a [REPLY v, s, x] message to the client.
                replica.getWrapper().forwardReply(new ReplyMessage(replica.getViewNumber(), request.getRequestNumber(), entry.getResult()));

                ballotRequestTable.remove(replica.getCommitNumber());
            } else {
                break;
            }
        }

        // TODO: make auto commit by timer
        // informs the backups of the latest commit by sending them a [COMMIT v, k] message
        if (changed) {
            replica.getWrapper().sendToOtherReplicas(new CommitMessage(replica.getViewNumber(), replica.getCommitNumber()));
        }
    }

    /**
     * Handles COMMIT message
     *
     * @param message COMMIT message
     */
    public void handleCommitMessage(@NotNull CommitMessage message) {
        // Replicas participate in processing of client requests only when their status is normal
        if (replica.getStatus() != ReplicaStatus.NORMAL) {
            return;
        }

        // If the sender is behind, the receiver drops the message
        if (message.getViewNumber() < replica.getViewNumber()) {
            return;
        }

        if (message.getViewNumber() > replica.getViewNumber()) {
            replica.startRecovery();
            return;
        }

        // Ignore committed operations
        if (message.getCommitNumber() <= replica.getCommitNumber()) {
            return;
        }

        commitUpTo(message.getCommitNumber());
    }

    private void executeNextOp() {
        RequestMessage request = replica.getLog().get(replica.getCommitNumber() + 1);
        ClientEntry entry = replica.getClient(request.getClientId());
        entry.setResult(upCall(request.getOperation()));
        entry.setProcessing(false);
        replica.setCommitNumber(replica.getCommitNumber() + 1);
    }

    private void commitUpTo(long primaryCommit) {
        while (replica.getCommitNumber() < primaryCommit && replica.getCommitNumber() < replica.getOperationNumber()) {
            executeNextOp();
        }
    }

    private String upCall(@NotNull Operation operation) {
        Utils.log(replica.getReplicaNumber(), "\"" + operation + "\" " + "is executed");
        return operation.delegateUpCall(replica);
    }
}
