package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import java.util.Arrays;

public class CommandRefresh implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.refresh(Arrays.asList(arguments));
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.refresh(keyspaces). Arguments is an array of keyspaces";
    }
}
