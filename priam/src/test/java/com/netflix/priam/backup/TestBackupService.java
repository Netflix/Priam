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

package com.netflix.priam.backup;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.PriamScheduler;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;

/** Created by aagrawal on 3/10/19. */
public class TestBackupService {
    private final PriamScheduler scheduler;
    private final InstanceIdentity instanceIdentity;

    public TestBackupService() {
        Injector injector = Guice.createInjector(new BRTestModule());
        this.scheduler = injector.getInstance(PriamScheduler.class);
        this.instanceIdentity = injector.getInstance(InstanceIdentity.class);
    }

    @Before
    public void cleanup() throws SchedulerException {
        scheduler.getScheduler().clear();
    }

    @Test
    public void testBackupDisabled(
            @Mocked IConfiguration configuration, @Mocked IBackupRestoreConfig backupRestoreConfig)
            throws Exception {
        new Expectations() {
            {
                configuration.getBackupCronExpression();
                result = "-1";
                configuration.getDataFileLocation();
                result = "data";
            }
        };
        IService backupService =
                new BackupService(configuration, backupRestoreConfig, scheduler, instanceIdentity);
        backupService.scheduleService();
        Assert.assertEquals(1, scheduler.getScheduler().getJobKeys(null).size());
    }

    @Test
    public void testBackupEnabled(
            @Mocked IConfiguration configuration, @Mocked IBackupRestoreConfig backupRestoreConfig)
            throws Exception {
        new Expectations() {
            {
                configuration.getBackupCronExpression();
                result = "0 0/1 * 1/1 * ? *";
                configuration.isIncrementalBackupEnabled();
                result = false;
            }
        };
        IService backupService =
                new BackupService(configuration, backupRestoreConfig, scheduler, instanceIdentity);
        backupService.scheduleService();
        Assert.assertEquals(2, scheduler.getScheduler().getJobKeys(null).size());
    }

    @Test
    public void testBackupEnabledWithIncremental(
            @Mocked IConfiguration configuration, @Mocked IBackupRestoreConfig backupRestoreConfig)
            throws Exception {
        new Expectations() {
            {
                configuration.getBackupCronExpression();
                result = "0 0/1 * 1/1 * ? *";
                configuration.isIncrementalBackupEnabled();
                result = true;
            }
        };
        IService backupService =
                new BackupService(configuration, backupRestoreConfig, scheduler, instanceIdentity);
        backupService.scheduleService();
        Assert.assertEquals(3, scheduler.getScheduler().getJobKeys(null).size());
    }

    // TEST CASES
    /*
    1. Disabling backup 1.0 only. Incremental might still be ON because of backup v2.
    2. Disabling backups all together.
    3. Disabling incremental backup only.
    4. Enabling backup 1.0
    5. Enabling incremental backups with 1.0
    6. Update Service - Write new config file and enable/disable via JMX.
     */
}
