package com.netflix.priam.scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NamedThreadPoolExecutor extends ThreadPoolExecutor
{
    public NamedThreadPoolExecutor(int poolSize, String poolName)
    {
        this(poolSize, poolName, new LinkedBlockingQueue<Runnable>());
    }

    public NamedThreadPoolExecutor(int poolSize, String poolName, BlockingQueue<Runnable> queue)
    {
        super(poolSize, poolSize, 1000, TimeUnit.MILLISECONDS, queue,
                new NamedThreadFactory(poolName), new LocalRejectedExecutionHandler(queue));
    }

    private static class LocalRejectedExecutionHandler implements RejectedExecutionHandler
    {
        private final BlockingQueue<Runnable> queue;

        LocalRejectedExecutionHandler(BlockingQueue<Runnable> queue)
        {
            this.queue = queue;
        }

        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor)
        {
            while (true)
            {
                if (executor.isShutdown())
                    throw new RejectedExecutionException("ThreadPoolExecutor has shut down");

                try
                {
                    if (queue.offer(task, 1000, TimeUnit.MILLISECONDS))
                        break;
                }
                catch (InterruptedException e)
                {
                    //NOP
                }
            }
        }
    }
}
