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

package com.netflix.priam.backup;

import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCustomizedTPE {
    private static final Logger logger = LoggerFactory.getLogger(TestCustomizedTPE.class);
    private static final int MAX_THREADS = 10;
    // timeout 1 sec
    private static final int TIME_OUT = 10 * 1000;
    private final BlockingSubmitThreadPoolExecutor startTest =
            new BlockingSubmitThreadPoolExecutor(
                    MAX_THREADS, new LinkedBlockingDeque<>(MAX_THREADS), TIME_OUT);

    @Test
    public void testExecutor() throws InterruptedException {
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 100; i++) {
            startTest.submit(
                    (Callable<Void>)
                            () -> {
                                Thread.sleep(100);
                                logger.info("Count:{}", count.incrementAndGet());
                                return null;
                            });
        }
        startTest.sleepTillEmpty();
        Assert.assertEquals(100, count.get());
    }

    @Test
    public void testException() {
        boolean success = false;
        try {
            for (int i = 0; i < 100; i++) {
                startTest.submit(
                        (Callable<Void>)
                                () -> {
                                    logger.info("Sleeping for 2 * timeout.");
                                    Thread.sleep(TIME_OUT * 2);
                                    return null;
                                });
            }
        } catch (RuntimeException ex) {
            success = true;
        }
        Assert.assertTrue("Failure to timeout...", success);
    }
}
