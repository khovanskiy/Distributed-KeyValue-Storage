package com.khovanskiy.dkvstorage.vr.replica;


import com.khovanskiy.dkvstorage.vr.ReplicaStatus;
import com.khovanskiy.dkvstorage.vr.Utils;
import com.khovanskiy.dkvstorage.vr.message.DoViewChangeMessage;
import com.khovanskiy.dkvstorage.vr.message.StartViewChangeMessage;
import com.khovanskiy.dkvstorage.vr.message.StartViewMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of ViewChange protocol
 *
 * @author Victor Khovanskiy
 */
public class ViewChangeState {
    private final Replica replica;
    private final List<StartViewChangeMessage> startViewChangeMessages = new ArrayList<>();
    private final List<DoViewChangeMessage> doViewChangeMessages = new ArrayList<>();
    private long lastNormalViewNumber;

    public ViewChangeState(Replica replica) {
        this.replica = replica;
    }

    public int getNextPrimaryNumber(long v) {
        return replica.getConfiguration().get((int) (v % replica.getConfiguration().size())).getReplicaNumber();
    }

    public void startViewChange() {
        long newViewNumber = replica.getViewNumber() + 1;

        // Replica is in recovering state. Ignoring the startViewChange
        if (replica.getStatus() == ReplicaStatus.RECOVERING) {
            return;
        }

        if (replica.getViewNumber() < newViewNumber) {
            startViewChangeMessages.clear();
            doViewChangeMessages.clear();

            if (replica.getStatus() == ReplicaStatus.NORMAL) {
                replica.setStatus(ReplicaStatus.VIEW_CHANGE);
                lastNormalViewNumber = replica.getViewNumber();
                replica.setViewNumber(newViewNumber);
                replica.getWrapper().sendToOtherReplicas(new StartViewChangeMessage(newViewNumber, replica.getReplicaNumber()));
            }
        }
    }

    public void processViewChangeMessage(StartViewChangeMessage event) {
        //Utils.log(replica.getReplicaNumber(), "received from #" + event.getReplicaNumber() + " " + Message.encode(event));
        long newViewNumber = event.getViewNumber();

        if (replica.getViewNumber() > newViewNumber) {
            return;
        }

        // Replica is in recovering state. Ignoring the startViewChange
        if (replica.getStatus() == ReplicaStatus.RECOVERING) {
            return;
        }

        if (replica.getViewNumber() < newViewNumber) {
            startViewChangeMessages.clear();
            doViewChangeMessages.clear();

            if (replica.getStatus() == ReplicaStatus.NORMAL) {
                replica.setStatus(ReplicaStatus.VIEW_CHANGE);
                lastNormalViewNumber = replica.getViewNumber();
                replica.setViewNumber(newViewNumber);
                replica.getWrapper().sendToOtherReplicas(new StartViewChangeMessage(newViewNumber, replica.getReplicaNumber()));
            }
        }

        startViewChangeMessages.add(event);

        int f = replica.getConfiguration().size() / 2;

        // When replica receives START_VIEW_CHANGE messages for its view-number from f other replicas
        if (f <= startViewChangeMessages.size()) {
            // it sends a [DOVIEWCHANGE v, l, lv, n, k, i] message to the node that will be the primary in the new view
            int newPrimary = getNextPrimaryNumber(replica.getViewNumber());
            DoViewChangeMessage doViewChange = new DoViewChangeMessage(replica.getViewNumber(), lastNormalViewNumber, replica.getOperationNumber(), replica.getCommitNumber(), replica.getReplicaNumber(), replica.getLog());
            if (replica.getReplicaNumber() != newPrimary) {
                replica.getWrapper().sendToReplica(newPrimary, doViewChange);
            } else {
                replica.onReceivedDoViewChange(doViewChange);
            }
        }
    }

    public void processDoViewChangeMessage(DoViewChangeMessage event) {
        //Utils.log(replica.getReplicaNumber(), "received from #" + event.getReplicaNumber() + " " + Message.encode(event));
        long newViewNumber = event.getViewNumber();

        if (replica.getViewNumber() > newViewNumber) {
            return;
        }

        // Replica is in recovering state. Ignoring the doViewChange
        if (replica.getStatus() == ReplicaStatus.RECOVERING) {
            return;
        }

        if (replica.getViewNumber() < newViewNumber) {
            startViewChangeMessages.clear();
            doViewChangeMessages.clear();

            if (replica.getStatus() == ReplicaStatus.NORMAL) {
                replica.setStatus(ReplicaStatus.VIEW_CHANGE);
                lastNormalViewNumber = replica.getViewNumber();
                replica.setViewNumber(newViewNumber);
                replica.getWrapper().sendToOtherReplicas(new StartViewChangeMessage(newViewNumber, replica.getReplicaNumber()));
            }
        }

        if (replica.getStatus() == ReplicaStatus.NORMAL) {
            return;
        }

        doViewChangeMessages.add(event);

        int f = replica.getConfiguration().size() / 2;

        // the new primary receives f + 1 DO_VIEW_CHANGE messages from different replicas (including itself)
        if (doViewChangeMessages.size() >= f + 1) {
            Utils.log(replica.getReplicaNumber(), "Im a new leader!");
            long largestLastNormalViewNumber = Integer.MIN_VALUE;
            long largestOperationNumber = Integer.MIN_VALUE;
            long largestCommitNumber = Integer.MIN_VALUE;
            for (DoViewChangeMessage message : doViewChangeMessages) {
                // it sets its view-number to that in the messages
                replica.setViewNumber(message.getViewNumber());

                // selects as the new log the one contained in the message with the largest lv
                if (largestLastNormalViewNumber < message.getLastNormalViewNumber()) {
                    replica.setLog(message.getLog());
                    largestLastNormalViewNumber = message.getLastNormalViewNumber();
                    largestOperationNumber = message.getOperationNumber();
                } else if (largestLastNormalViewNumber == message.getLastNormalViewNumber()) {
                    // if several messages have the same last-normal-view-number it selects the one among them with the largest operation-number
                    if (largestOperationNumber < message.getOperationNumber()) {
                        replica.setLog(message.getLog());
                        largestOperationNumber = message.getOperationNumber();
                    }
                }

                //  It sets its operation-number to that of the topmost entry in the new log
                replica.setOperationNumber(largestOperationNumber);

                // sets its commit-number to the largest such number it received in the DOVIEWCHANGE messages
                if (largestCommitNumber < message.getCommitNumber()) {
                    largestCommitNumber = message.getCommitNumber();
                    replica.setCommitNumber(largestCommitNumber);
                }
            }

            // changes its status to normal
            replica.setStatus(ReplicaStatus.NORMAL);

            // and informs the other replicas of the completion of the view change by sending [STARTVIEW v, l, n, k] messages to the other replicas
            replica.getWrapper().sendToOtherReplicas(new StartViewMessage(replica.getViewNumber(), replica.getOperationNumber(), replica.getCommitNumber(), replica.getLog()));
        }
    }

    public void processStartViewMessage(StartViewMessage event) {
        //Utils.log(replica.getReplicaNumber(), "received from #" + getNextPrimaryNumber(event.getViewNumber()) + " " + Message.encode(event));

        // replace their log with the one in the message
        replica.setLog(event.getLog());

        // , set their op-number to that of the latest entry in the log
        replica.setOperationNumber(event.getOperationNumber());

        // set their view-number to the view number in the message
        replica.setViewNumber(event.getViewNumber());

        // change their status to normal
        replica.setStatus(ReplicaStatus.NORMAL);

        // and update the information in their client-table

    }
}
