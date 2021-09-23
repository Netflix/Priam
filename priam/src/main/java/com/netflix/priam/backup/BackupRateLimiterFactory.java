package com.netflix.priam.backup;

import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.inject.Inject;

public class BackupRateLimiterFactory implements RateLimiterFactory {

    private final Clock clock;

    @Inject
    BackupRateLimiterFactory(Clock clock) {
        this.clock = clock;
    }

    @Override
    public RateLimiter create(AbstractBackupPath path, Instant target) {
        long secondsRemaining = Duration.between(clock.instant(), target).getSeconds();
        if (secondsRemaining < 1) {
            // skip file system checks when unnecessary
            return RateLimiter.create(Double.MAX_VALUE);
        }
        long totalBytes = getTotalSize(path.getBackupDirectory());
        double permitsPerSecond =
                totalBytes < 1 ? Double.MAX_VALUE : (double) totalBytes / secondsRemaining;
        return RateLimiter.create(permitsPerSecond);
    }

    private long getTotalSize(Path dir) {
        BackupFileVisitor fileVisitor = new BackupFileVisitor();
        try {
            Files.walkFileTree(dir, fileVisitor);
        } catch (IOException e) {
            // BackupFileVisitor is happy with an estimate and won't produce these in practice.
        }
        return fileVisitor.getTotalBytes();
    }

    private static final class BackupFileVisitor implements FileVisitor<Path> {
        private long totalBytes;

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            totalBytes += attrs.size();
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
