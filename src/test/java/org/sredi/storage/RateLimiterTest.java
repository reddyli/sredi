package org.sredi.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RateLimiterTest {

    @Test
    void allowsRequestsUpToCapacity() {
        RateLimiter limiter = new RateLimiter(5);
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.tryConsume(), "Request " + i + " should be allowed");
        }
        assertFalse(limiter.tryConsume(), "Request beyond capacity should be rejected");
    }

    @Test
    void refillsTokensOverTime() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(10);
        // Drain all tokens
        for (int i = 0; i < 10; i++) {
            limiter.tryConsume();
        }
        assertFalse(limiter.tryConsume());

        // Wait for refill
        Thread.sleep(500);
        assertTrue(limiter.tryConsume(), "Should have refilled after waiting");
    }

    @Test
    void doesNotExceedCapacity() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(3);
        // Wait long enough to theoretically refill beyond capacity
        Thread.sleep(2000);

        int allowed = 0;
        for (int i = 0; i < 10; i++) {
            if (limiter.tryConsume()) allowed++;
        }
        assertEquals(3, allowed, "Should not exceed capacity even after long wait");
    }
}

