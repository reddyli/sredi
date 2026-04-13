package org.sredi.storage;

/**
 * Rate limiting via token bucket algorithm
 * capacity same as max burst (rps) , we can probably separate burst and capacity later.
 */
public class RateLimiter {

    private final long capacity;
    private final double refillRatePerMs;

    private double availableTokens;
    private long lastRefillTimeMs;

    public RateLimiter(long maxRequestsPerSecond) {
        this.capacity = maxRequestsPerSecond;
        this.refillRatePerMs = maxRequestsPerSecond / 1000.0;
        this.availableTokens = capacity;
        this.lastRefillTimeMs = System.currentTimeMillis();
    }

    public boolean tryConsume() {
        refill();
        if (availableTokens >= 1) {
            availableTokens--;
            return true;
        }
        return false;
    }

    private void refill() {
        long now = System.currentTimeMillis();
        long elapsedMs = now - lastRefillTimeMs;
        if (elapsedMs > 0) {
            availableTokens = Math.min(capacity, availableTokens + elapsedMs * refillRatePerMs);
            lastRefillTimeMs = now;
        }
    }
}
