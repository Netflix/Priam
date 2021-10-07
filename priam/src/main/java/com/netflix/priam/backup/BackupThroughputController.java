package com.netflix.priam.backup;

import com.netflix.priam.config.IConfiguration;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.inject.Inject;

public class BackupThroughputController implements ThroughputController {

    private final Clock clock;
    private final IConfiguration config;

    @Inject
    BackupThroughputController(IConfiguration config, Clock clock) {
        this.clock = clock;
        this.config = config;
    }

    @Override
    public double getDesiredThroughput(AbstractBackupPath path, Instant target) {
        long secondsRemaining = Duration.between(clock.instant(), target).getSeconds();
        if (secondsRemaining < 1) {
            // skip file system checks when unnecessary
            return Double.MAX_VALUE;
        }
        long totalBytes = getTotalSize();
        return totalBytes < 1 ? Double.MAX_VALUE : (double) totalBytes / secondsRemaining;
    }

    private long getTotalSize() {
        BackupFileVisitor fileVisitor = new BackupFileVisitor();
        try {
            Files.walkFileTree(Paths.get(config.getDataFileLocation()), fileVisitor);
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
