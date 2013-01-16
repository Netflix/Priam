package com.netflix.priam.agent.process;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

public class ProcessRecord
{
    private final String            name;
    private final String            id;
    private final List<String>      arguments;
    private final long              startTimeMs = System.currentTimeMillis();

    private volatile long           endTimeMs = -1;
    private volatile long           stopAttemptMs = 0;
    private volatile Future<Void>   executor;
    private volatile boolean wasForceStopped = false;

    ProcessRecord(String name, String id, String[] arguments)
    {
        this.id = id;
        this.arguments = ImmutableList.copyOf(Arrays.asList(arguments));
        this.name = name;
    }

    void setExecutor(Future<Void> executor)
    {
        this.executor = executor;
    }

    Future<Void> getExecutor()
    {
        return executor;
    }

    void setEnd(boolean wasForceStopped)
    {
        executor = null;
        endTimeMs = System.currentTimeMillis();
        this.wasForceStopped = wasForceStopped;
    }

    void noteStopAttempt()
    {
        stopAttemptMs = System.currentTimeMillis();
    }

    public String getName()
    {
        return name;
    }

    public long getStartTimeMs()
    {
        return startTimeMs;
    }

    public String getId()
    {
        return id;
    }

    public List<String> getArguments()
    {
        return arguments;
    }

    public long getEndTimeMs()
    {
        return endTimeMs;
    }

    public long getStopAttemptMs()
    {
        return stopAttemptMs;
    }

    public boolean wasForceStopped()
    {
        return wasForceStopped;
    }
}
