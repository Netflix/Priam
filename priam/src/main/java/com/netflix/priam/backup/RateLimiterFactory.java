package com.netflix.priam.backup;

import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.ImplementedBy;
import java.time.Instant;

@ImplementedBy(BackupRateLimiterFactory.class)
public interface RateLimiterFactory {
    RateLimiter create(AbstractBackupPath dir, Instant target);
}
