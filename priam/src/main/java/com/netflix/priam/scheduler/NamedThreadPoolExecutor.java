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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.*;

public class NamedThreadPoolExecutor extends ThreadPoolExecutor {
    public NamedThreadPoolExecutor(int poolSize, String poolName) {
        this(poolSize, poolName, new LinkedBlockingQueue<>());
    }

    public NamedThreadPoolExecutor(int poolSize, String poolName, BlockingQueue<Runnable> queue) {
        super(
                poolSize,
                poolSize,
                1000,
                TimeUnit.MILLISECONDS,
                queue,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat(poolName + "-%d").build(),
                new LocalRejectedExecutionHandler(queue));
    }

    private static class LocalRejectedExecutionHandler implements RejectedExecutionHandler {
        private final BlockingQueue<Runnable> queue;

        LocalRejectedExecutionHandler(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            while (true) {
                if (executor.isShutdown())
                    throw new RejectedExecutionException("ThreadPoolExecutor has shut down");

                try {
                    if (queue.offer(task, 1000, TimeUnit.MILLISECONDS)) break;
                } catch (InterruptedException e) {
                    // NOP
                }
            }
        }
    }
}
