package com.netflix.priam.agent.process;

import com.netflix.priam.IConfiguration;
import java.util.concurrent.Callable;

class Executor implements Callable<Void>
{
    private final AgentProcess process;
    private final IConfiguration configuration;
    private final String[] arguments;

    private volatile Thread     thread;

    Executor(AgentProcess process, IConfiguration configuration, String[] arguments)
    {
        this.process = process;
        this.configuration = configuration;
        this.arguments = arguments;
    }

    void    interruptTask()
    {
        if ( thread != null )
        {
            thread.interrupt();
        }
    }

    @Override
    public Void call() throws Exception
    {
        thread = Thread.currentThread();
        try
        {
            process.performCommand(configuration, arguments);
        }
        finally
        {
            thread = null;
        }
        return null;
    }
}
