package com.netflix.priam.agent.process;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Maintains state of a process
 */
public class ProcessRecord
{
    private final String            name;
    private final String            id;
    private final List<String>      arguments;
    private final long              startTimeMs = System.currentTimeMillis();

    private volatile long           endTimeMs = 0;
    private volatile long           stopAttemptMs = 0;
    private volatile Future<Void>   executor;

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

    void setEnd()
    {
        executor = null;
        endTimeMs = System.currentTimeMillis();
    }

    void noteStopAttempt()
    {
        stopAttemptMs = System.currentTimeMillis();
    }

    /**
     * @return the process name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return when the process was started
     */
    public long getStartTimeMs()
    {
        return startTimeMs;
    }

    /**
     * @return the process's ID
     */
    public String getId()
    {
        return id;
    }

    /**
     * @return the arguments
     */
    public List<String> getArguments()
    {
        return arguments;
    }

    /**
     * @return when the process ended or 0
     */
    public long getEndTimeMs()
    {
        return endTimeMs;
    }

    /**
     * @return when the process was attempted to be stopped or 0
     */
    public long getStopAttemptMs()
    {
        return stopAttemptMs;
    }
}
