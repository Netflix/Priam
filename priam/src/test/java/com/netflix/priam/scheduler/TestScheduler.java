/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.priam.scheduler;

import com.google.inject.Inject;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerFactory;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ArchaiusModule.class, BRTestModule.class})
public class TestScheduler {
    // yuck, but marginally better than using Thread.sleep
    private static CountDownLatch latch;
    @Inject private PriamScheduler scheduler;

    @Test
    public void testSchedule() throws Exception {
        latch = new CountDownLatch(1);
        scheduler.start();
        scheduler.addTask("test", TestTask.class, new SimpleTimer("testtask", 10));
        // verify the task has run or fail in 1s
        latch.await(1000, TimeUnit.MILLISECONDS);
        scheduler.deleteTask("test");
    }

    public static class TestTask extends Task {
        @Inject
        public TestTask(IConfiguration config) {
            // todo: mock the MBeanServer instead, but this will prevent exceptions due to duplicate
            // registrations
            super(config, MBeanServerFactory.newMBeanServer());
        }

        @Override
        public void execute() {
            latch.countDown();
        }

        @Override
        public String getName() {
            return "test";
        }
    }
}
