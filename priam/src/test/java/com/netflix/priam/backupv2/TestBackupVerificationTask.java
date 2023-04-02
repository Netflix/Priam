/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.backupv2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.*;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.merics.Metrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.utils.DateUtil.DateRange;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import javax.inject.Inject;
import mockit.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/** Created by aagrawal on 2/1/19. */
public class TestBackupVerificationTask {
    @Inject private BackupVerificationTask backupVerificationService;
    private Counter badVerifications;
    @Mocked private BackupVerification backupVerification;
    @Mocked private BackupNotificationMgr backupNotificationMgr;

    @Before
    public void setUp() {
        new MockBackupVerification();
        new MockBackupNotificationMgr();
        Injector injector = Guice.createInjector(new BRTestModule());
        injector.injectMembers(this);
        badVerifications =
                injector.getInstance(Registry.class)
                        .counter(Metrics.METRIC_PREFIX + "backup.verification.failure");
        MockBackupVerification.clearMissingFiles();
    }

    private static final class MockBackupVerification extends MockUp<BackupVerification> {
        private static boolean throwError;
        private static final Map<BackupMetadata, ImmutableSet<String>> verifiedBackups;

        static {
            verifiedBackups = new HashMap<>();
        }

        public static void updateMissingFiles(
                BackupMetadata newVerifiedBackups, ImmutableSet<String> missingFiles) {
            verifiedBackups.put(newVerifiedBackups, missingFiles);
        }

        public static void clearMissingFiles() {
            verifiedBackups.clear();
        }

        public static void shouldThrow(boolean newThrowError) {
            throwError = newThrowError;
        }

        @Mock
        public ImmutableMap<BackupMetadata, ImmutableSet<String>> findMissingBackupFilesInRange(
                BackupVersion backupVersion, boolean force, DateRange dateRange)
                throws UnsupportedTypeException, IllegalArgumentException {
            if (throwError) throw new IllegalArgumentException("DummyError");
            return ImmutableMap.copyOf(verifiedBackups);
        }
    }

    private static final class MockBackupNotificationMgr extends MockUp<BackupNotificationMgr> {}

    @Test
    public void throwError() {
        MockBackupVerification.shouldThrow(true);
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> backupVerificationService.execute());
    }

    @Test
    public void validBackups() throws Exception {
        MockBackupVerification.shouldThrow(false);
        MockBackupVerification.updateMissingFiles(
                getRecentlyValidatedMetadata(), ImmutableSet.of());
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(0);
        new Verifications() {
            {
                backupNotificationMgr.notify(anyString, (Instant) any);
                times = 1;
            }
        };
    }

    @Test
    public void invalidBackups() throws Exception {
        MockBackupVerification.shouldThrow(false);
        MockBackupVerification.updateMissingFiles(
                getInvalidBackupMetadata(), ImmutableSet.of("foo"));
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(1);
        new Verifications() {
            {
                backupNotificationMgr.notify(anyString, (Instant) any);
                times = 0;
            }
        };
    }

    @Test
    public void previouslyVerifiedBackups() throws Exception {
        MockBackupVerification.shouldThrow(false);
        MockBackupVerification.updateMissingFiles(
                getPreviouslyValidatedMetadata(), ImmutableSet.of());
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(0);
        new Verifications() {
            {
                backupNotificationMgr.notify(anyString, (Instant) any);
                times = 0;
            }
        };
    }

    @Test
    public void noBackups() throws Exception {
        MockBackupVerification.shouldThrow(false);
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(1);
        new Verifications() {
            {
                backupNotificationMgr.notify(anyString, (Instant) any);
                maxTimes = 0;
            }
        };
    }

    @Test
    public void testRestoreMode(@Mocked InstanceState state) throws Exception {
        new Expectations() {
            {
                state.getRestoreStatus().getStatus();
                result = Status.STARTED;
            }
        };
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(0);
        new Verifications() {
            {
                backupVerification.findMissingBackupFilesInRange(
                        (BackupVersion) any, anyBoolean, (DateRange) any);
                maxTimes = 0;
            }

            {
                backupNotificationMgr.notify(anyString, (Instant) any);
                maxTimes = 0;
            }
        };
    }

    private static BackupMetadata getInvalidBackupMetadata() {
        return new BackupMetadata(BackupVersion.SNAPSHOT_META_SERVICE, "12345", new Date());
    }

    private static BackupMetadata getPreviouslyValidatedMetadata() {
        BackupMetadata backupMetadata =
                new BackupMetadata(BackupVersion.SNAPSHOT_META_SERVICE, "12345", new Date());
        backupMetadata.setLastValidated(
                new Date(Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()));
        return backupMetadata;
    }

    private static BackupMetadata getRecentlyValidatedMetadata() {
        BackupMetadata backupMetadata =
                new BackupMetadata(BackupVersion.SNAPSHOT_META_SERVICE, "12345", new Date());
        backupMetadata.setLastValidated(
                new Date(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli()));
        return backupMetadata;
    }
}
