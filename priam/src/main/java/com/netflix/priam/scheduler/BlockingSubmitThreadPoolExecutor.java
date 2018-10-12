/*
 * Copyright 2013 Netflix, Inc.
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

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ThreadPoolExecutor} that will block in the {@code submit()} method until the task can be
 * successfully added to the queue.
 */
public class BlockingSubmitThreadPoolExecutor extends ThreadPoolExecutor {
    private static final long DEFAULT_SLEEP = 100;
    private static final long DEFAULT_KEEP_ALIVE = 100;
    private static final Logger logger =
            LoggerFactory.getLogger(BlockingSubmitThreadPoolExecutor.class);
    private final BlockingQueue<Runnable> queue;
    private final long giveupTime;
    private final AtomicInteger active;

    public BlockingSubmitThreadPoolExecutor(
            int maximumPoolSize, BlockingQueue<Runnable> workQueue, long timeoutAdding) {
        super(maximumPoolSize, maximumPoolSize, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS, workQueue);
        this.queue = workQueue;
        this.giveupTime = timeoutAdding;
        this.active = new AtomicInteger(0);
    }

    /**
     * This is a thread safe way to avoid rejection exception... this is implemented because we
     * might want to hold the incoming requests till there is a free thread.
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        synchronized (this) {
            active.incrementAndGet();
            long timeout = 0;
            while (queue.remainingCapacity() == 0) {
                try {
                    if (timeout <= giveupTime) {
                        Thread.sleep(DEFAULT_SLEEP);
                        timeout += DEFAULT_SLEEP;
                    } else {
                        throw new RuntimeException("Timed out because TPE is too busy...");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.submit(task);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        active.decrementAndGet();
    }

    /** blocking call to test if the threads are done or not. */
    public void sleepTillEmpty() {
        long timeout = 0;

        while (!queue.isEmpty() || (active.get() > 0)) {
            try {
                if (timeout <= giveupTime) {
                    Thread.sleep(DEFAULT_SLEEP);
                    timeout += DEFAULT_SLEEP;
                    logger.debug(
                            "After Sleeping for empty: {}, Count: {}", +queue.size(), active.get());
                } else {
                    throw new RuntimeException("Timed out because TPE is too busy...");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
