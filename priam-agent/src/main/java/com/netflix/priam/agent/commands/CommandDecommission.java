package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;

public class CommandDecommission implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.decommission();
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.decommission()";
    }
}
