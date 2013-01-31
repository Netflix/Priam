package com.netflix.priam.agent.tasks;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.Task;
import javax.management.MBeanServer;

public class ProcessTaskWrapper extends Task
{
    private final ProcessTask task;

    public ProcessTaskWrapper(ProcessTask task, IConfiguration config)
    {
        super(config);
        this.task = task;
    }

    public ProcessTaskWrapper(ProcessTask task, IConfiguration config, MBeanServer mBeanServer)
    {
        super(config, mBeanServer);
        this.task = task;
    }

    @Override
    public void execute() throws Exception
    {
        task.execute();
    }

    @Override
    public String getName()
    {
        return "Process Task";
    }
}
