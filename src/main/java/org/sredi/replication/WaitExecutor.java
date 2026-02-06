package org.sredi.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.resp.RespValue;

/**
 * Alternative implementation for handling WAIT command (currently unused - see ReplConfAckManager).
 *
 * Sends REPLCONF GETACK to all followers in parallel using an ExecutorService,
 * then waits for responses using a CountDownLatch. Includes extended timeout
 * logic for compatibility with certain test scenarios.
 *
 * Note: The current implementation uses ReplConfAckManager instead, which uses
 * a different approach based on object locks and notification.
 */
public class WaitExecutor {

    private static final Logger log = LoggerFactory.getLogger(WaitExecutor.class);

    // Extra time beyond requested timeout to wait for stragglers
    private static final long EXTENDED_TIMEOUT_MS = 100L;

    // Polling interval while waiting for additional ACKs
    private static final long POLL_INTERVAL_MS = 100L;

    private final int requestedAckCount;
    private final int numToWaitFor;
    private final AtomicInteger acknowledgedCount;
    private final CountDownLatch latch;
    private final ExecutorService executorService;

    public WaitExecutor(int requestedAckCount, int numFollowers, ExecutorService executorService) {
        this.requestedAckCount = requestedAckCount;
        this.numToWaitFor = Math.min(requestedAckCount, numFollowers);
        this.executorService = executorService;
        this.acknowledgedCount = new AtomicInteger(0);
        this.latch = new CountDownLatch(requestedAckCount);
    }

    // Sends GETACK to all followers and waits for responses
    public int wait(Collection<ConnectionToFollower> followers, long timeoutMillis) {
        long extendedTimeout = timeoutMillis + EXTENDED_TIMEOUT_MS;
        List<Future<Void>> ackRequestFutures = submitAckRequests(followers, extendedTimeout);

        try {
            waitForAcksWithExtendedTimeout(followers.size(), timeoutMillis, extendedTimeout);
        } catch (Exception e) {
            log.error("Error waiting for ACKs: received {} of {} requested", acknowledgedCount.get(), numToWaitFor);
        } finally {
            cancelPendingRequests(ackRequestFutures);
        }

        log.debug("Returning {} of {} requested acks", acknowledgedCount.get(), requestedAckCount);
        return acknowledgedCount.get();
    }

    // Submits ACK requests to all followers in parallel
    private List<Future<Void>> submitAckRequests(Collection<ConnectionToFollower> followers, long timeout) {
        List<Future<Void>> futures = new ArrayList<>();
        for (ConnectionToFollower follower : followers) {
            futures.add(executorService.submit(() -> {
                requestAckFromFollower(follower, timeout);
                return null;
            }));
        }
        return futures;
    }

    // Waits for ACKs with extended timeout for test compatibility
    private void waitForAcksWithExtendedTimeout(int followerCount, long timeoutMillis, long extendedTimeout)
            throws Exception {
        long startTime = System.currentTimeMillis();
        log.debug("Waiting up to {} ms for {} ACKs", extendedTimeout, numToWaitFor);

        // Submit the waiting logic as an async task and block on it
        executorService.submit(() -> {
            try {
                long deadline = calculateDeadline(startTime, timeoutMillis, extendedTimeout);
                waitUntilDeadlineOrAllAcked(followerCount, deadline);
            } catch (InterruptedException e) {
                log.debug("Interrupted while waiting for ACKs, received {}", acknowledgedCount.get());
            }
        }).get();

        log.debug("Wait completed in {} ms", System.currentTimeMillis() - startTime);
    }

    // Calculates deadline based on whether initial timeout was met
    private long calculateDeadline(long startTime, long timeoutMillis, long extendedTimeout)
            throws InterruptedException {
        if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            log.debug("Initial timeout expired with {} ACKs, extending wait", acknowledgedCount.get());
            return startTime + extendedTimeout;
        }
        // Got enough ACKs, but wait full timeout for additional ones (test compatibility)
        return startTime + timeoutMillis;
    }

    // Polls until deadline or all followers have acknowledged
    private void waitUntilDeadlineOrAllAcked(int followerCount, long deadline) throws InterruptedException {
        while (acknowledgedCount.get() < followerCount && System.currentTimeMillis() < deadline) {
            log.debug("Have {} ACKs, waiting until {}", acknowledgedCount.get(), deadline);
            Thread.sleep(POLL_INTERVAL_MS);
        }
    }

    // Sends GETACK to a single follower and records response
    private void requestAckFromFollower(ConnectionToFollower follower, long timeout) {
        try {
            log.debug("Requesting ACK from {}", follower);
            RespValue response = follower.sendAndWaitForReplConfAck(timeout);

            if (response == null) {
                log.debug("No response from {}, still waiting for {} ACKs",
                        follower, numToWaitFor - acknowledgedCount.get());
                return;
            }

            int newCount = acknowledgedCount.incrementAndGet();
            latch.countDown();
            log.debug("Received ACK #{} from {}: {}",
                    newCount, follower, Command.responseLogString(response.asResponse()));

        } catch (Exception e) {
            log.error("Error requesting ACK from {}: {} {}",
                    follower, e.getClass().getSimpleName(), e.getMessage());
        }
    }

    // Cancels any pending ACK request tasks
    private void cancelPendingRequests(List<Future<Void>> futures) {
        int cancelled = 0;
        for (Future<Void> future : futures) {
            if (future.cancel(true)) {
                cancelled++;
            }
        }
        log.debug("Cancelled {} of {} pending tasks", cancelled, futures.size());
    }

    @Override
    public String toString() {
        return "WaitExecutor[waiting=" + numToWaitFor + ", acked=" + acknowledgedCount.get() + "]";
    }
}
