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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.TestModule;
import com.netflix.priam.config.IConfiguration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestScheduler {
    // yuck, but marginally better than using Thread.sleep
    private static CountDownLatch latch;

    @Test
    public void testSchedule() throws Exception {
        latch = new CountDownLatch(1);
        Injector inject = Guice.createInjector(new TestModule());
        PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
        scheduler.start();
        scheduler.addTask("test", TestTask.class, new SimpleTimer("testtask", 10));
        // verify the task has run or fail in 1s
        latch.await(1000, TimeUnit.MILLISECONDS);
        scheduler.shutdown();
    }

    @Test
    @Ignore(
            "not sure what this test really does, except test countdown latch and thread context switching")
    public void testSingleInstanceSchedule() throws Exception {
        latch = new CountDownLatch(3);
        Injector inject = Guice.createInjector(new TestModule());
        PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
        scheduler.start();
        scheduler.addTask("test2", SingleTestTask.class, SingleTestTask.getTimer());
        // verify 3 tasks run or fail in 1s
        latch.await(2000, TimeUnit.MILLISECONDS);
        scheduler.shutdown();
        Assert.assertEquals(3, SingleTestTask.count);
    }

    @Ignore
    public static class TestTask extends Task {
        @Inject
        public TestTask(IConfiguration config) {
            // todo: mock the MBeanServer instead, but this will prevent exceptions due to duplicate
            // registrations
            super(config);
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

    @Ignore
    @Singleton
    public static class SingleTestTask extends Task {
        @Inject
        public SingleTestTask(IConfiguration config) {
            super(config);
        }

        static int count = 0;

        @Override
        public void execute() {
            ++count;
            latch.countDown();
            try {
                // todo : why is this sleep important?
                Thread.sleep(55); // 5sec
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        @Override
        public String getName() {
            return "test2";
        }

        static TaskTimer getTimer() {
            return new SimpleTimer("test2", 11L);
        }
    }
}
