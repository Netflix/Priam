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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.BackupVerification;
import com.netflix.priam.backup.BackupVerificationResult;
import com.netflix.priam.backup.BackupVersion;
import com.netflix.priam.backup.Status;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.util.ArrayList;
import java.util.List;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

/** Created by aagrawal on 2/1/19. */
public class TestBackupVerificationTask {
    private static BackupVerificationTask backupVerificationService;
    private static IConfiguration configuration;
    private static BackupVerification backupVerification;

    public TestBackupVerificationTask() {
        new MockBackupVerification();
        Injector injector = Guice.createInjector(new BRTestModule());
        if (configuration == null) configuration = injector.getInstance(IConfiguration.class);
        if (backupVerificationService == null)
            backupVerificationService = injector.getInstance(BackupVerificationTask.class);
        if (backupVerification == null)
            backupVerification = injector.getInstance(BackupVerification.class);
    }

    static class MockBackupVerification extends MockUp<BackupVerification> {
        public static boolean failCall = false;
        public static boolean throwError = false;

        @Mock
        public List<BackupVerificationResult> verifyAllBackups(
                BackupVersion backupVersion, boolean force, DateRange dateRange)
                throws UnsupportedTypeException, IllegalArgumentException {
            if (throwError) throw new IllegalArgumentException("DummyError");

            if (failCall) return new ArrayList<>();

            BackupVerificationResult backupVerificationResult = new BackupVerificationResult();
            backupVerificationResult.valid = true;
            List<BackupVerificationResult> result = new ArrayList<>();
            return result;
        }
    }

    @Test
    public void throwError() throws Exception {
        MockBackupVerification.throwError = true;
        MockBackupVerification.failCall = false;
        try {
            backupVerificationService.execute();
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().equalsIgnoreCase("DummyError")) Assert.assertTrue(false);
        }
    }

    @Test
    public void failCalls() throws Exception {
        MockBackupVerification.throwError = false;
        MockBackupVerification.failCall = true;
        backupVerificationService.execute();
    }

    @Test
    public void normalOperation() throws Exception {
        MockBackupVerification.throwError = false;
        MockBackupVerification.failCall = false;
        backupVerificationService.execute();
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
    }
}
