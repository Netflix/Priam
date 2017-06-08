package com.netflix.priam.backup;

import org.junit.Assert;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;

public class TestCustomizedTPE
{
    private static final Logger logger = LoggerFactory.getLogger(TestCustomizedTPE.class);
    private static final int MAX_THREADS = 10;
    // timeout 1 sec
    private static final int TIME_OUT = 10 * 1000;
    private BlockingSubmitThreadPoolExecutor startTest = new BlockingSubmitThreadPoolExecutor(MAX_THREADS, new LinkedBlockingDeque<Runnable>(MAX_THREADS), TIME_OUT);

    @Test
    public void testExecutor() throws InterruptedException
    {
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 100; i++)
        {
            startTest.submit(new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    Thread.sleep(100);
                    logger.info("Count:" + count.incrementAndGet());
                    return null;
                }
            });
        }
        startTest.sleepTillEmpty();
        Assert.assertEquals(100, count.get());
    }

    @Test
    public void testException()
    {
        boolean success = false;
        try
        {
            for (int i = 0; i < 100; i++)
            {
                startTest.submit(new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        logger.info("Sleeping for 2 * timeout.");
                        Thread.sleep(TIME_OUT * 2);                        
                        return null;
                    }
                });
            }
        }
        catch (RuntimeException ex)
        {
            success = true;
        }
        Assert.assertTrue("Failure to timeout...", success);
    }

}
