package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;

public class CommandStartGossiping implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.startGossiping();
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.startGossiping()";
    }
}
