package com.netflix.priam.agent.process;

import com.netflix.priam.agent.NodeStatus;
import java.util.concurrent.Callable;

class Executor implements Callable<Void>
{
    private final AgentProcessManager processManager;
    private final String id;
    private final AgentProcess process;
    private final NodeStatus nodeTool;
    private final String[] arguments;

    Executor(AgentProcessManager processManager, String id, AgentProcess process, NodeStatus nodeTool, String[] arguments)
    {
        this.processManager = processManager;
        this.id = id;
        this.process = process;
        this.nodeTool = nodeTool;
        this.arguments = arguments;
    }

    @Override
    public Void call() throws Exception
    {
        try
        {
            process.performCommand(nodeTool, arguments);
        }
        finally
        {
            processManager.removeProcess(id);
        }
        return null;
    }
}
