package com.netflix.priam.backup;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Instant;

public class FakeRateLimiterFactory implements RateLimiterFactory {
    @Override
    public RateLimiter create(AbstractBackupPath dir, Instant target) {
        return RateLimiter.create(Double.MAX_VALUE);
    }
}
