package com.netflix.priam.backup;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.priam.config.IConfiguration;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.inject.Inject;

public class BackupDynamicRateLimiter implements DynamicRateLimiter {

    private final Clock clock;
    private final IConfiguration config;
    private final DirectorySize dirSize;
    private final RateLimiter rateLimiter;

    @Inject
    public BackupDynamicRateLimiter(IConfiguration config, Clock clock, DirectorySize dirSize) {
        this.clock = clock;
        this.config = config;
        this.dirSize = dirSize;
        this.rateLimiter = RateLimiter.create(Double.MAX_VALUE);
    }

    @Override
    public void acquire(AbstractBackupPath path, Instant target, int permits) {
        if (target.equals(Instant.EPOCH)
                || !path.getBackupFile()
                        .getAbsolutePath()
                        .contains(AbstractBackup.SNAPSHOT_FOLDER)) {
            return;
        }
        long secondsRemaining = Duration.between(clock.instant(), target).getSeconds();
        if (secondsRemaining < 1) {
            // skip file system checks when unnecessary
            return;
        }
        int backupThreads = config.getBackupThreads();
        Preconditions.checkState(backupThreads > 0);
        long bytesPerThread = this.dirSize.getBytes(config.getDataFileLocation()) / backupThreads;
        if (bytesPerThread < 1) {
            return;
        }
        double newRate = (double) bytesPerThread / secondsRemaining;
        double oldRate = rateLimiter.getRate();
        if ((Math.abs(newRate - oldRate) / oldRate) > config.getRateLimitChangeThreshold()) {
            rateLimiter.setRate(newRate);
        }
        rateLimiter.acquire(permits);
    }
}
