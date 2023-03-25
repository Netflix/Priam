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

import com.google.common.collect.ImmutableList;
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
import java.util.List;
import java.util.Optional;
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
    }

    private static final class MockBackupVerification extends MockUp<BackupVerification> {
        private static boolean throwError;
        private static ImmutableList<BackupVerificationResult> results;

        public static void setResults(BackupVerificationResult... newResults) {
            results = ImmutableList.copyOf(newResults);
        }

        public static void shouldThrow(boolean newThrowError) {
            throwError = newThrowError;
        }

        @Mock
        public List<BackupVerificationResult> verifyAllBackups(
                BackupVersion backupVersion, DateRange dateRange)
                throws UnsupportedTypeException, IllegalArgumentException {
            if (throwError) throw new IllegalArgumentException("DummyError");
            return results;
        }

        @Mock
        public Optional<BackupVerificationResult> verifyBackup(
                BackupVersion backupVersion, boolean force, DateRange dateRange)
                throws UnsupportedTypeException, IllegalArgumentException {
            if (throwError) throw new IllegalArgumentException("DummyError");
            return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
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
        MockBackupVerification.setResults(getValidBackupVerificationResult());
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(0);
        new Verifications() {
            {
                backupNotificationMgr.notify((BackupVerificationResult) any);
                times = 1;
            }
        };
    }

    @Test
    public void invalidBackups() throws Exception {
        MockBackupVerification.shouldThrow(false);
        MockBackupVerification.setResults(getInvalidBackupVerificationResult());
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(0);
        new Verifications() {
            {
                backupNotificationMgr.notify((BackupVerificationResult) any);
                times = 1;
            }
        };
    }

    @Test
    public void noBackups() throws Exception {
        MockBackupVerification.shouldThrow(false);
        MockBackupVerification.setResults();
        backupVerificationService.execute();
        Truth.assertThat(badVerifications.count()).isEqualTo(1);
        new Verifications() {
            {
                backupNotificationMgr.notify((BackupVerificationResult) any);
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
                backupVerification.findLatestVerifiedBackup(
                        (BackupVersion) any, anyBoolean, (DateRange) any);
                maxTimes = 0;
            }

            {
                backupVerification.verifyBackupsInRange((BackupVersion) any, (DateRange) any);
                maxTimes = 0;
            }

            {
                backupNotificationMgr.notify((BackupVerificationResult) any);
                maxTimes = 0;
            }
        };
    }

    private static BackupVerificationResult getInvalidBackupVerificationResult() {
        BackupVerificationResult result = new BackupVerificationResult();
        result.valid = false;
        result.manifestAvailable = true;
        result.remotePath = "some_random";
        result.filesMatched = 123;
        result.snapshotInstant = Instant.EPOCH;
        return result;
    }

    private static BackupVerificationResult getValidBackupVerificationResult() {
        BackupVerificationResult result = new BackupVerificationResult();
        result.valid = true;
        result.manifestAvailable = true;
        result.remotePath = "some_random";
        result.filesMatched = 123;
        result.snapshotInstant = Instant.EPOCH;
        return result;
    }
}
