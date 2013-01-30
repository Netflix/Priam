package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ProcessMetaData;
import com.netflix.priam.agent.process.SimpleProcessMetaData;

public class CommandDrain implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.drain();
    }

    @Override
    public ProcessMetaData getMetaData()
    {
        return new SimpleProcessMetaData("Calls nodeTool.drain()");
    }
}
