package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.IdentificationMessage;
import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.message.MessageHandler;
import com.khovanskiy.dkvstorage.vr.message.MessageReply;
import com.khovanskiy.dkvstorage.vr.message.PrepareMessage;
import com.khovanskiy.dkvstorage.vr.message.PrepareOkMessage;
import com.khovanskiy.dkvstorage.vr.message.RequestMessage;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Victor Khovanskiy
 */
public class Replica {

    private final LinkedBlockingQueue<MessageEntry> messages = new LinkedBlockingQueue<>();
    private final ExecutorService backgroundExecutor = Executors.newCachedThreadPool();
    private final Runnable runnableExecutor = new Runnable() {
        @Override
        public void run() {
            while (true) {
                try {
                    MessageEntry entry = messages.take();
                    MessageHandler parser = new MessageHandler(entry.getRawMessage());
                    Message message = parser.parse();
                    trace("Process message " + message + " from connection " + entry.getSocketConnection());
                    // TODO: replace this workaround
                    if (message instanceof IdentificationMessage) {
                        onReceivedIdentification(entry.getSocketConnection(), (IdentificationMessage) message);
                    } else if (message instanceof RequestMessage) {
                        onReceivedRequest((RequestMessage) message);
                    } else if (message instanceof PrepareMessage) {
                        onReceivedPrepare((PrepareMessage) message);
                    } else if (message instanceof PrepareOkMessage) {
                        onReceivedPrepareOk((PrepareOkMessage) message);
                    }
                } catch (InterruptedException ignored) {
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    };
    /**
     * This is a sorted array containing the 2f + 1 replicas.
     */
    private List<Replica> configuration;
    /**
     * This is the index into the configuration where this replica is stored.
     */
    private final int replicaNumber;
    /**
     * The current view-number, initially 0.
     */
    private int viewNumber = 0;
    /**
     * The current status, either @code{ReplicaStatus.NORMAL}, @code{ReplicaStatus.VIEW_CHANGE}, or @code{ReplicaStatus.RECOVERING}
     */
    private ReplicaStatus status;
    /**
     * This is assigned to the most recently received request, initially 0.
     */
    private int opNumber = 0;
    /**
     * This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
     */
    private final List<LogEntry> log = new ArrayList<>();
    /**
     * This is the op-number of the most recently committed operation.
     */
    private int commitNumber;
    /**
     * This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
     */
    private final Map<Integer, ClientEntry> clientTable = new HashMap<>();


    private final Map<Integer, Map<Integer, Boolean>> ballotTable = new HashMap<>();
    private int quorumSize;

    //private Logger logger = Logger.getLogger(Replica.class.getName());
    private int primaryNumber;
    private final String host;
    private final int port;
    private List<SocketConnection> outgoing;
    private Map<Integer, SocketConnection> incoming;
    private Set<SocketConnection> unknowns;
    private ServerSocket serverSocket;
    public Map<String, String> storage = new HashMap<>();

    private final Runnable runnableAccepter = new Runnable() {
        @Override
        public void run() {
            while (true) {
                //trace("Try accept...");
                try {
                    Socket socket = serverSocket.accept();
                    onAccept(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    };

    private final SocketConnection.ConnectionListener incomingListener = new SocketConnection.ConnectionListener() {
        @Override
        public void onDisconnected(SocketConnection socketConnection, @Nullable IOException e) {
            trace("Disconnected " + socketConnection);
        }

        @Override
        public void onConnected(SocketConnection socketConnection) {
            trace("Connected " + socketConnection);
        }

        @Override
        public void onReceived(SocketConnection socketConnection, @NotNull String line) {
            trace("Replica received \"" + line + "\" from incoming connection " + socketConnection);
            try {
                messages.put(new MessageEntry(socketConnection, line));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    };
    private final SocketConnection.ConnectionListener outgoingListener = new SocketConnection.ConnectionListener() {
        @Override
        public void onDisconnected(SocketConnection socketConnection, @Nullable IOException e) {
            trace("Disconnected " + socketConnection);
        }

        @Override
        public void onConnected(SocketConnection socketConnection) {
            trace("Connected to outgoing " + socketConnection);
            sendTo(socketConnection, new IdentificationMessage(getReplicaNumber()));
        }

        @Override
        public void onReceived(SocketConnection socketConnection, @NotNull String line) {
            //trace("Replica received \"" + line + "\" from outgoing connection " + connection);
            try {
                messages.put(new MessageEntry(socketConnection, line));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    };

    public Replica(int replicaNumber, String host, int port) throws IOException {
        this.replicaNumber = replicaNumber;
        this.host = host;
        this.port = port;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public void start(List<Replica> configuration) throws IOException {
        //trace("Replica " + toString() + " starting...");
        this.quorumSize = configuration.size() / 2 + 1;
        this.status = ReplicaStatus.NORMAL;
        this.primaryNumber = configuration.get(0).getReplicaNumber();
        this.configuration = configuration;
        //this.serverSocket = new ServerSocket(getPort());

        ConnectionSelector connectionSelector = new ConnectionSelector(getHost(), getPort());

        for (Replica replica : configuration) {
            if (replica.getReplicaNumber() == getReplicaNumber()) {
                continue;
            }
            ConnectionSelector.Connection connection = new ConnectionSelector.Connection(replica.getHost(), replica.getPort());
            connection.setKeepConnection(true);
            connectionSelector.connect(connection);
            break;
        }

        connectionSelector.start();

        /*
        this.outgoing = new ArrayList<>(this.configuration.size() - 1);
        this.incoming = new HashMap<>(this.configuration.size());
        this.unknowns = new HashSet<>(this.configuration.size());

        //trace("Creating outgoing connections to other replicas: ");
        int k = 1;
        for (Replica replica : this.configuration) {
            if (replica.getReplicaNumber() == replicaNumber) {
                continue;
            }
            Connection connection = new Connection(replica.getHost(), replica.getPort());
            connection.setKeepConnection(true);
            connection.setConnectionListener(outgoingListener);
            outgoing.connect(connection);
            //trace(k++ + ") Created connection to " + replica);
        }

        //trace("Starting outgoing connections to other replicas: ");
        k = 1;
        for (Connection connection : outgoing) {
            connection.start();
            //trace(k++ + ") Started connection " + connection);
        }

        //trace("Launch executor and accepter runnables");
        backgroundExecutor.submit(runnableExecutor);
        backgroundExecutor.submit(runnableAccepter);*/
    }

    private synchronized void trace(String s) {
        System.out.println(s);
    }

    private synchronized void sendTo(SocketConnection socketConnection, Message message) {
        socketConnection.send(message.toString());
    }

    private synchronized void sendOtherReplicas(Message message) {
        for (SocketConnection socketConnection : outgoing) {
            socketConnection.send(message.toString());
        }
    }

    private synchronized void onAccept(@NotNull Socket socket) {
        SocketConnection socketConnection = new SocketConnection(socket);
        //trace("Accept " + connection);
        unknowns.add(socketConnection);
        socketConnection.setConnectionListener(incomingListener);
        socketConnection.start();
    }


    public void onReceivedIdentification(SocketConnection socketConnection, IdentificationMessage identification) {
        if (unknowns.contains(socketConnection)) {
            incoming.put(identification.getId(), socketConnection);
            unknowns.remove(socketConnection);
            sendTo(socketConnection, new MessageReply("ACCEPTED"));
        }
    }

    public void onReceivedRequest(RequestMessage request) {
        if (!isPrimary()) {
            return;
        }

        ClientEntry entry = clientTable.get(request.getClientId());
        if (entry != null) {
            // If the request-number s isn’t bigger than the information in the table
            if (request.getRequestNumber() < entry.getRequest().getRequestNumber()) {
                // drops stale request
                return;
            }
            // if the request is the most recent one from this client and it has already been executed
            if (request.getRequestNumber() == entry.getRequest().getRequestNumber()) {
                if (entry.isProcessing()) {
                    // still processing request
                    return;
                } else {
                    // re-send the response
                    replyClient(request.getClientId(), entry.getReply());
                }
            }
        }

        // advances op-number
        opNumber++;

        // adds the request to the end of the log
        log.add(new LogEntry(request));

        // updates the information for this client in the client-table to contain the new request number
        clientTable.put(request.getClientId(), new ClientEntry(request));

        // prepare table for ballot by voting backups
        ballotTable.put(opNumber, new HashMap<>());

        // sends a <PREPARE v, m, n, k> message to the other replicas
        Message prepare = new PrepareMessage(viewNumber, request, opNumber, commitNumber);
        sendOtherReplicas(prepare);
    }

    public void replyClient(int clientId, MessageReply reply) {
        // TODO
    }

    public void onReceivedPrepare(PrepareMessage prepare) {
        if (prepare.getViewNumber() > viewNumber) {
            //If this node also thinks it is a prmary, will do recovery
            //go s.StartRecovery()
            return;
        }

        // TODO: Handle msg from older views
        if (prepare.getViewNumber() < viewNumber) {
            return;
        }

        if (prepare.getOpNumber() > opNumber + 1) {
            //go s.StartRecovery()
            return;
        }

        // Ignore out-of-order message
        if (prepare.getOpNumber() < opNumber + 1) {
            return;
        }

        // won’t accept a prepare with op-number n until it has entries for all earlier requests in its log.
        commitUpTo(prepare.getCommitNumber());

        //increments its op-number
        opNumber++;

        // adds the request to the end of its log
        log.add(new LogEntry(prepare.getRequest()));

        clientTable.put(prepare.getRequest().getClientId(), new ClientEntry(prepare.getRequest()));
    }

    public void onReceivedPrepareOk(PrepareOkMessage prepareOk) {
        // Ignore commited operations
        if (prepareOk.getOpNumber() <= commitNumber) {
            return;
        }

        // Update table for PrepareOK messages
        Map<Integer, Boolean> ballot = ballotTable.get(prepareOk.getOpNumber());
        if (ballot == null) {
            return;
        }
        ballot.put(prepareOk.getBackupNumber(), true);

        // Commit operations agreed by quorum
        if (prepareOk.getOpNumber() > commitNumber + 1) {
            return;
        }

        while (commitNumber < opNumber) {
            ballot = ballotTable.get(commitNumber + 1);
            if (ballot.size() + 1 >= quorumSize) {
                executeNextOp();
                ballotTable.remove(commitNumber);
            } else {
                break;
            }
        }
    }

    private void executeNextOp() {
        RequestMessage request = log.get(commitNumber + 1).getRequest();
        clientTable.get(request.getClientId()).setReply(upCall(request.getOperation()));
        clientTable.get(request.getClientId()).setProcessing(false);
        ++commitNumber;
    }

    private void commitUpTo(int primaryCommit) {
        while (commitNumber < primaryCommit && commitNumber < opNumber) {
            executeNextOp();
        }
    }

    private MessageReply upCall(Operation operation) {
        trace(operation + " " + "is executed");
        return operation.delegateUpCall(this);
    }

    public boolean isPrimary() {
        return getReplicaNumber() == primaryNumber;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return getHost() + ":" + getPort();
    }

    private class MessageEntry {
        private SocketConnection socketConnection;
        private String message;

        public MessageEntry(SocketConnection socketConnection, String message) {
            this.socketConnection = socketConnection;
            this.message = message;
        }

        public SocketConnection getSocketConnection() {
            return socketConnection;
        }

        public String getRawMessage() {
            return message;
        }
    }
}
