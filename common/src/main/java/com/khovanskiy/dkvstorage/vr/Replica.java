package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.*;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.*;
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
                    trace("Process message " + message + " from connection " + entry.getConnection());
                    if (message instanceof IdentificationMessage) {
                        onReceivedIdentification(entry.getConnection(), (IdentificationMessage) message);
                    } else if (message instanceof MessageRequest) {
                        onReceivedRequest((MessageRequest) message);
                    } else if (message instanceof MessagePrepare) {
                        onReceivedPrepare((MessagePrepare) message);
                    }
                } catch (InterruptedException ignored) {
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    };
    public List<Replica> configuration;
    private final int replicaNumber;
    public int viewNumber;
    public ReplicaStatus status;
    public int opNumber;
    public List<LogEntry> log = new ArrayList<>();
    public int commitNumber;
    public Map<Integer, ClientEntry> clientTable = new HashMap<>();

    //private Logger logger = Logger.getLogger(Replica.class.getName());
    private int primaryNumber;
    private final String host;
    private final int port;
    private List<Connection> outgoing;
    private Map<Integer, Connection> incoming;
    private Set<Connection> unknowns;
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
    private final Connection.ConnectionListener incomingListener = new Connection.ConnectionListener() {
        @Override
        public void onDisconnected(Connection connection, @Nullable IOException e) {
            trace("Disconnected " + connection);
        }

        @Override
        public void onConnected(Connection connection) {
            trace("Connected " + connection);
        }

        @Override
        public void onReceived(Connection connection, @NotNull String line) {
            trace("Replica received \"" + line + "\" from incoming connection " + connection);
            try {
                messages.put(new MessageEntry(connection, line));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    };
    private final Connection.ConnectionListener outgoingListener = new Connection.ConnectionListener() {
        @Override
        public void onDisconnected(Connection connection, @Nullable IOException e) {
            trace("Disconnected " + connection);
        }

        @Override
        public void onConnected(Connection connection) {
            trace("Connected to outgoing " + connection);
            sendTo(connection, new IdentificationMessage(getReplicaNumber()));
        }

        @Override
        public void onReceived(Connection connection, @NotNull String line) {
            //trace("Replica received \"" + line + "\" from outgoing connection " + connection);
            try {
                messages.put(new MessageEntry(connection, line));
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
        this.primaryNumber = configuration.get(0).getReplicaNumber();
        this.configuration = configuration;
        this.serverSocket = new ServerSocket(getPort());

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
            outgoing.add(connection);
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
        backgroundExecutor.submit(runnableAccepter);
    }

    private synchronized void trace(String s) {
        System.out.println(s);
    }

    private synchronized void sendTo(Connection connection, Message message) {
        connection.send(message.toString());
    }

    private synchronized void sendOtherReplicas(Message message) {
        for (Connection connection : outgoing) {
            connection.send(message.toString());
        }
    }

    private synchronized void onAccept(@NotNull Socket socket) {
        Connection connection = new Connection(socket);
        //trace("Accept " + connection);
        unknowns.add(connection);
        connection.setConnectionListener(incomingListener);
        connection.start();
    }


    public void onReceivedIdentification(Connection connection, IdentificationMessage identification) {
        if (unknowns.contains(connection)) {
            incoming.put(identification.getId(), connection);
            unknowns.remove(connection);
            sendTo(connection, new MessageReply("ACCEPTED"));
        }
    }

    public void onReceivedRequest(MessageRequest request) {
        if (!isPrimary()) {
            return;
        }

        ClientEntry entry = clientTable.get(request.getClientId());
        if (entry != null) {
            // If the request-number s isn’t bigger than the information in the table
            if (request.getRequestNumber() < entry.getRequest().getRequestNumber()) {
                // drops
                return;
            }
            // if the request is the most recent one from this client and it has already been executed
            if (request.getRequestNumber() == entry.getRequest().getRequestNumber() && entry.hasExecuted()) {
                // re-send the response

            }
        }

        // advances op-number
        opNumber++;

        // adds the request to the end of the log
        log.add(new LogEntry(request));

        // updates the information for this client in the client-table to contain the new request number
        clientTable.put(request.getClientId(), new ClientEntry(request));

        commitUpTo(opNumber);

        // sends a <PREPARE v, m, n, k> message to the other replicas
        Message prepare = new MessagePrepare(viewNumber, request, opNumber, commitNumber);
        //sendOtherReplicas(prepare);
    }

    public void onReceivedPrepare(MessagePrepare prepare) {
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

        commitUpTo(prepare.getCommitNumber());

        //increments its op-number
        opNumber++;

        // adds the request to the end of its log
        log.add(new LogEntry(prepare.getRequest()));

        clientTable.put(prepare.getRequest().getClientId(), new ClientEntry(prepare.getRequest()));
    }

    private void commitUpTo(int primaryCommit) {
        // won’t accept a prepare with op-number n until it has entries for all earlier requests in its log.
        while (commitNumber < primaryCommit && commitNumber < opNumber) {
            MessageRequest request = log.get(commitNumber + 1).getRequest();
            clientTable.get(request.getClientId()).setReply(upCall(request.getOperation()));
            ++commitNumber;
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
        private Connection connection;
        private String message;

        public MessageEntry(Connection connection, String message) {
            this.connection = connection;
            this.message = message;
        }

        public Connection getConnection() {
            return connection;
        }

        public String getRawMessage() {
            return message;
        }
    }
}
