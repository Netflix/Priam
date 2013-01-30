package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ProcessMetaData;
import com.netflix.priam.agent.process.SimpleProcessMetaData;

public class CommandRemoveNode implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.removeNode(arguments[0]);
    }

    @Override
    public ProcessMetaData getMetaData()
    {
        return new SimpleProcessMetaData("Calls nodeTool.removeNode(token). Argument is the token to remove.", 1);
    }
}
