package com.netflix.priam.backup;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.config.FakeConfiguration;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBackupDynamicRateLimiter {
    private static final Instant NOW = Instant.ofEpochMilli(1 << 16);
    private static final Instant LATER = NOW.plusMillis(Duration.ofHours(1).toMillis());
    private static final int DIR_SIZE = 1 << 16;

    private BackupDynamicRateLimiter rateLimiter;
    private FakeConfiguration config;
    private Injector injector;

    @Before
    public void setUp() {
        injector = Guice.createInjector(new BRTestModule());
        config = injector.getInstance(FakeConfiguration.class);
    }

    @Test
    public void sunnyDay() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, DIR_SIZE);
        Stopwatch timer = timePermitAcquisition(getBackupPath(), LATER, 21);
        Truth.assertThat(timer.elapsed(TimeUnit.MILLISECONDS)).isAtLeast(1_000);
        Truth.assertThat(timer.elapsed(TimeUnit.MILLISECONDS)).isAtMost(2_000);
    }

    @Test
    public void targetSetToEpoch() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, DIR_SIZE);
        Stopwatch timer = timePermitAcquisition(getBackupPath(), Instant.EPOCH, 20);
        assertNoRateLimiting(timer);
    }

    @Test
    public void pathIsNotASnapshot() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, DIR_SIZE);
        AbstractBackupPath path =
                getBackupPath(
                        "target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-4-Data.db");
        Stopwatch timer = timePermitAcquisition(path, LATER, 20);
        assertNoRateLimiting(timer);
    }

    @Test
    public void targetIsNow() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, DIR_SIZE);
        Stopwatch timer = timePermitAcquisition(getBackupPath(), NOW, 20);
        assertNoRateLimiting(timer);
    }

    @Test
    public void targetIsInThePast() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, DIR_SIZE);
        Instant target = NOW.minus(Duration.ofHours(1L));
        Stopwatch timer = timePermitAcquisition(getBackupPath(), target, 20);
        assertNoRateLimiting(timer);
    }

    @Test
    public void noBackupThreads() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 0), NOW, DIR_SIZE);
        Assert.assertThrows(
                IllegalStateException.class,
                () -> timePermitAcquisition(getBackupPath(), LATER, 20));
    }

    @Test
    public void negativeBackupThreads() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", -1), NOW, DIR_SIZE);
        Assert.assertThrows(
                IllegalStateException.class,
                () -> timePermitAcquisition(getBackupPath(), LATER, 20));
    }

    @Test
    public void noData() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, 0);
        Stopwatch timer = timePermitAcquisition(getBackupPath(), LATER, 20);
        assertNoRateLimiting(timer);
    }

    @Test
    public void noPermitsRequested() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, DIR_SIZE);
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> timePermitAcquisition(getBackupPath(), LATER, 0));
    }

    @Test
    public void negativePermitsRequested() {
        rateLimiter = getRateLimiter(ImmutableMap.of("Priam.backup.threads", 1), NOW, DIR_SIZE);
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> timePermitAcquisition(getBackupPath(), LATER, -1));
    }

    private RemoteBackupPath getBackupPath() {
        return getBackupPath(
                "target/data/Keyspace1/Standard1/snapshots/snap_v2_202201010000/.STANDARD1_field1_idx_1/Keyspace1-Standard1-ia-4-Data.db");
    }

    private RemoteBackupPath getBackupPath(String filePath) {
        RemoteBackupPath path = injector.getInstance(RemoteBackupPath.class);
        path.parseLocal(Paths.get(filePath).toFile(), AbstractBackupPath.BackupFileType.SST_V2);
        return path;
    }

    private Stopwatch timePermitAcquisition(AbstractBackupPath path, Instant now, int permits) {
        rateLimiter.acquire(path, now, permits); // Do this once first or else it won't throttle.
        Stopwatch timer = Stopwatch.createStarted();
        rateLimiter.acquire(path, now, permits);
        timer.stop();
        return timer;
    }

    private BackupDynamicRateLimiter getRateLimiter(
            Map<String, Object> properties, Instant now, long directorySize) {
        properties.forEach(config::setFakeConfig);
        return new BackupDynamicRateLimiter(
                config,
                Clock.fixed(now, ZoneId.systemDefault()),
                new FakeDirectorySize(directorySize));
    }

    private void assertNoRateLimiting(Stopwatch timer) {
        Truth.assertThat(timer.elapsed(TimeUnit.MILLISECONDS)).isAtMost(1);
    }

    private static final class FakeDirectorySize implements DirectorySize {
        private final long size;

        FakeDirectorySize(long size) {
            this.size = size;
        }

        @Override
        public long getBytes(String location) {
            return size;
        }
    }
}
