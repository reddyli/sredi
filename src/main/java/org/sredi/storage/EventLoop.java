package org.sredi.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.CommandConstructor;

/**
 * Main event loop that polls for client commands and executes them.
 * Runs on the main thread and processes commands sequentially.
 */
public class EventLoop {
    private static final Logger log = LoggerFactory.getLogger(EventLoop.class);
    private static final long IDLE_SLEEP_MILLIS = 80L;

    private final CentralRepository repository;
    private final CommandConstructor commandConstructor;
    private volatile boolean shutdownRequested = false;

    public EventLoop(CentralRepository repository, CommandConstructor commandConstructor) {
        this.repository = repository;
        this.commandConstructor = commandConstructor;
    }

    public void terminate() {
        shutdownRequested = true;
    }

    public void runCommandLoop() throws InterruptedException {
        while (!shutdownRequested) {
            boolean didProcess = processNextCommand();

            if (!didProcess) {
                Thread.sleep(IDLE_SLEEP_MILLIS);
            }
        }
    }

    private boolean processNextCommand() {
        return repository.getConnectionManager().getNextValue((conn, value) -> {
            Command command = commandConstructor.newCommandFromValue(value);
            if (command != null) {
                try {
                    repository.executeCommand(conn, command);
                } catch (Exception e) {
                    log.error("EventLoop Exception: {} \"{}\"", 
                            e.getClass().getSimpleName(), e.getMessage(), e);
                    conn.sendError(e.getMessage());
                }
            }
        });
    }
}

