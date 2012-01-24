package com.priam.utils;

import java.util.concurrent.CancellationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExponentialRetryCallable<T> extends RetryableCallable<T>
{
    private static final Logger logger = LoggerFactory.getLogger(ExponentialRetryCallable.class);
    public final static long MAX_SLEEP = 240000;
    public final static long MIN_SLEEP = 200;

    private long max;
    private long min;

    public ExponentialRetryCallable()
    {
        this.max = MAX_SLEEP;
        this.min = MIN_SLEEP;
    }

    public ExponentialRetryCallable(long minSleep, long maxSleep)
    {
        this.max = maxSleep;
        this.min = minSleep;
    }

    public T call() throws Exception
    {
        long delay = min;// ms
        while (true)
        {
            try
            {
                return retriableCall();
            }
            catch (CancellationException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                logger.error(e.getMessage(), e);
                delay *= 2;
                if (delay > max)
                {
                    throw e;
                }
                Thread.sleep(delay);
            }
            finally
            {
                forEachExecution();
            }
        }
    }

}
