package com.netflix.priam.scheduler;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory
{
    protected final String name;
    protected final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String name)
    {
        this.name = name;
    }

    public Thread newThread(Runnable runnable)
    {
        String localName = name + ":" + n.getAndIncrement();
        Thread thread = new Thread(runnable, localName);
        thread.setDaemon(true);
        return thread;
    }
}
