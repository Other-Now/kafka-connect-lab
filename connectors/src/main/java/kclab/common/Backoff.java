package kclab.common;

import java.util.concurrent.ThreadLocalRandom;

/** Exponential backoff with full jitter: sleep = rand(0, min(cap, base * 2^attempt)). */
public final class Backoff {
    private final long baseMs;
    private final long capMs;

    public Backoff(long baseMs, long capMs) {
        this.baseMs = baseMs;
        this.capMs = capMs;
    }

    public long delayMs(int attempt) {
        long ceiling = Math.min(capMs, baseMs << Math.min(attempt, 20));
        return ThreadLocalRandom.current().nextLong(ceiling + 1);
    }
}
