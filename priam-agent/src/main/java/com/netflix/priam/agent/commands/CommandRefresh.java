package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ProcessMetaData;
import com.netflix.priam.agent.process.SimpleProcessMetaData;
import java.util.Arrays;

public class CommandRefresh implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.refresh(Arrays.asList(arguments));
    }

    @Override
    public ProcessMetaData getMetaData()
    {
        return new SimpleProcessMetaData("Calls nodeTool.refresh(keyspaces). Each argument is a keyspace to refresh.", 1);
    }
}
