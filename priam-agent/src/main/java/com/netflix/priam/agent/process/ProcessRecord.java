package com.netflix.priam.agent.process;

public class ProcessRecord
{
    private final String            name;
    private final String            id;
    private final long              startTimeMs = System.currentTimeMillis();

    private volatile AgentProcess   process;
    private volatile Executor       executor;

    ProcessRecord(String name, String id)
    {
        this.id = id;
        this.process = null;
        this.name = name;
    }

    void setProcess(AgentProcess process, Executor executor)
    {
        this.process = process;
        this.executor = executor;
    }

    Executor getExecutor()
    {
        return executor;
    }

    public AgentProcess getProcess()
    {
        return process;
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
}
