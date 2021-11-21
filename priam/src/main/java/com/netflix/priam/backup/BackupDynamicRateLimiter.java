package com.netflix.priam.backup;

import com.google.common.util.concurrent.RateLimiter;
import com.netflix.priam.config.IConfiguration;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.inject.Inject;

public class BackupDynamicRateLimiter implements DynamicRateLimiter {

    private final Clock clock;
    private final IConfiguration config;
    private final RateLimiter rateLimiter;

    @Inject
    BackupDynamicRateLimiter(IConfiguration config, Clock clock) {
        this.clock = clock;
        this.config = config;
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
        long bytesRemaining = getBytesRemaining();
        if (bytesRemaining < 1) {
            return;
        }
        double newRate = (double) bytesRemaining / secondsRemaining;
        double oldRate = rateLimiter.getRate();
        if ((Math.abs(newRate - oldRate) / oldRate) > config.getRateLimitChangeThreshold()) {
            rateLimiter.setRate(newRate);
        }
        rateLimiter.acquire(permits);
    }

    private long getBytesRemaining() {
        BackupFileVisitor fileVisitor = new BackupFileVisitor();
        try {
            Files.walkFileTree(Paths.get(config.getDataFileLocation()), fileVisitor);
        } catch (IOException e) {
            // BackupFileVisitor is happy with an estimate and won't produce these in practice.
        }
        return fileVisitor.getTotalBytes() / config.getBackupThreads();
    }

    private static final class BackupFileVisitor implements FileVisitor<Path> {
        private long totalBytes;

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.toString().contains(AbstractBackup.SNAPSHOT_FOLDER) && attrs.isRegularFile()) {
                totalBytes += attrs.size();
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            return FileVisitResult.CONTINUE;
        }

        long getTotalBytes() {
            return totalBytes;
        }
    }
}
