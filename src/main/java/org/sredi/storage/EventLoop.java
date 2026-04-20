package org.sredi.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.CommandConstructor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Main event loop that polls for client commands and executes them.
 * Runs on the main thread and processes commands sequentially.
 */
public class EventLoop {
    private static final Logger log = LoggerFactory.getLogger(EventLoop.class);
    private static final long IDLE_SLEEP_MILLIS = 80L;

    private final Orchestrator orchestrator;
    private final CommandConstructor commandConstructor;
    private volatile boolean shutdownRequested = false;

    private final boolean parallel;
    private final ExecutorService parallelCommandExecutorService;
    private final StripedLock stripedLock;

    public EventLoop(Orchestrator orchestrator, CommandConstructor commandConstructor) {
        this.orchestrator = orchestrator;
        this.commandConstructor = commandConstructor;
        this.parallel = orchestrator.getOptions().isParallel();

        if (parallel) {
            int threads = orchestrator.getOptions().getParallelThreads();
            this.parallelCommandExecutorService = Executors.newFixedThreadPool(threads);
            this.stripedLock = new StripedLock();
            log.info("Parallel mode enabled with {} threads", threads);
        } else {
            this.parallelCommandExecutorService = null;
            this.stripedLock = null;
        }
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
        return orchestrator.getConnectionManager().getNextValue((conn, value) -> {
            Command command = commandConstructor.newCommandFromValue(value);
            if (command != null) {
                if (parallel) {
                    parallelCommandExecutorService.submit(() -> {
                        String key = command.getKey();
                        try {
                            if (key != null) {
                                if (command.getType().isWrite()) {
                                    stripedLock.writeLock(key);
                                } else {
                                    stripedLock.readLock(key);
                                }
                            }
                            orchestrator.executeCommand(conn, command);
                        } catch (Exception e) {
                            log.error("EventLoop Exception: {} \"{}\"",
                                    e.getClass().getSimpleName(), e.getMessage(), e);
                            conn.sendError(e.getMessage());
                        } finally {
                            if (key != null) {
                                if (command.getType().isWrite()) {
                                    stripedLock.writeUnlock(key);
                                } else {
                                    stripedLock.readUnlock(key);
                                }
                            }
                        }
                    });
                } else {
                    try {
                        orchestrator.executeCommand(conn, command);
                    } catch (Exception e) {
                        log.error("EventLoop Exception: {} \"{}\"",
                                e.getClass().getSimpleName(), e.getMessage(), e);
                        conn.sendError(e.getMessage());
                    }
                }
            }
        });
    }
}

