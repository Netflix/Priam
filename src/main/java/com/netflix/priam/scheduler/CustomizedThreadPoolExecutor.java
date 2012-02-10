package com.netflix.priam.scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomizedThreadPoolExecutor extends ThreadPoolExecutor
{
    private static final long DEFAULT_SLEEP = 100;
    private static final long DEFAULT_KEEP_ALIVE = 100;
    private static final Logger logger = LoggerFactory.getLogger(CustomizedThreadPoolExecutor.class);
    private BlockingQueue<Runnable> queue;
    private long giveupTime;
    private AtomicInteger active;

    public CustomizedThreadPoolExecutor(int maximumPoolSize, BlockingQueue<Runnable> workQueue, long timeoutAdding)
    {
        super(maximumPoolSize, maximumPoolSize, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS, workQueue);
        this.queue = workQueue;
        this.giveupTime = timeoutAdding;
        this.active = new AtomicInteger(0);
    }

    /**
     * This is a thread safe way to avoid rejection exception... this is
     * implemented because we might want to hold the incoming requests till
     * there is a free thread.
     */
    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
        synchronized (this)
        {
            active.incrementAndGet();
            long timeout = 0;
            while (queue.remainingCapacity() == 0)
            {
                try
                {
                    if (timeout <= giveupTime)
                    {
                        Thread.sleep(DEFAULT_SLEEP);
                        timeout += DEFAULT_SLEEP;
                    }
                    else
                    {
                        throw new RuntimeException("Timed out because TPE is too busy...");
                    }
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return super.submit(task);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r, t);
        active.decrementAndGet();
    }

    /**
     * blocking call to test if the threads are done or not.
     */
    public void sleepTillEmpty()
    {
        long timeout = 0;

        while (!queue.isEmpty() || (active.get() > 0))
        {
            try
            {
                if (timeout <= giveupTime)
                {
                    Thread.sleep(DEFAULT_SLEEP);
                    timeout += DEFAULT_SLEEP;
                    logger.debug("After Sleeping for empty: {}, Count: {}", +queue.size(), active.get());
                }
                else
                {
                    throw new RuntimeException("Timed out because TPE is too busy...");
                }
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

    }
}
