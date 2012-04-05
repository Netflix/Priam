package com.netflix.priam.utils;

/**
 * Sleeper impl that delegates to Thread.sleep
 */
public class ThreadSleeper implements Sleeper
{
    @Override
    public void sleep(long waitTimeMs) throws InterruptedException
    {
        Thread.sleep(waitTimeMs);
    }
}