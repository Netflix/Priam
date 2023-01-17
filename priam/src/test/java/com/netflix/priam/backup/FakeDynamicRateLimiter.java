package com.netflix.priam.backup;

import java.time.Instant;

public class FakeDynamicRateLimiter implements DynamicRateLimiter {
    @Override
    public void acquire(AbstractBackupPath dir, Instant target, int tokens) {}
}
