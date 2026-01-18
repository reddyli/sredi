package org.sredi.storage;

import org.sredi.commands.Command;
import org.sredi.replication.ClientConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages MULTI/EXEC/DISCARD transactions per client connection.
 * Each connection can have at most one active transaction.
 */
public class TransactionManager {
    private final Map<ClientConnection, List<Command>> transactionQueues = new ConcurrentHashMap<>();
    private final ConnectionProvider connectionProvider;
    private final CommandExecutor commandExecutor;

    @FunctionalInterface
    public interface ConnectionProvider {
        ClientConnection getCurrentConnection();
    }

    @FunctionalInterface
    public interface CommandExecutor {
        byte[] execute(Command command);
    }

    public TransactionManager(ConnectionProvider connectionProvider, CommandExecutor commandExecutor) {
        this.connectionProvider = connectionProvider;
        this.commandExecutor = commandExecutor;
    }

    public void startTransaction() {
        ClientConnection conn = requireCurrentConnection();
        transactionQueues.put(conn, new ArrayList<>());
    }

    public void queueCommand(Command command) {
        ClientConnection conn = requireCurrentConnection();
        List<Command> queue = transactionQueues.get(conn);
        if (queue == null) {
            throw new IllegalStateException("No active transaction");
        }
        queue.add(command);
    }

    public byte[][] executeTransaction() {
        ClientConnection conn = requireCurrentConnection();
        List<Command> queue = transactionQueues.remove(conn);
        if (queue == null) {
            return null; // No active transaction
        }

        byte[][] results = new byte[queue.size()][];
        for (int i = 0; i < queue.size(); i++) {
            try {
                results[i] = commandExecutor.execute(queue.get(i));
            } catch (Exception e) {
                transactionQueues.remove(conn);
                throw e;
            }
        }
        return results;
    }

    public void discardTransaction() {
        ClientConnection conn = requireCurrentConnection();
        transactionQueues.remove(conn);
    }

    public boolean hasActiveTransaction(ClientConnection conn) {
        return transactionQueues.containsKey(conn);
    }

    private ClientConnection requireCurrentConnection() {
        ClientConnection conn = connectionProvider.getCurrentConnection();
        if (conn == null) {
            throw new IllegalStateException("No active connection for transaction");
        }
        return conn;
    }
}

