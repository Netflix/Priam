package com.netflix.priam.backup;

import com.google.inject.ImplementedBy;
import java.time.Instant;

@ImplementedBy(BackupDynamicRateLimiter.class)
public interface DynamicRateLimiter {
    void acquire(AbstractBackupPath dir, Instant target, int tokens);
}
