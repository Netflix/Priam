package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;

public class CommandMove implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        if ( arguments.length == 0 )
        {
            throw new IllegalStateException("a token argument must be provided");
        }
        nodeTool.move(arguments[0]);
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.move(token). Argument is the token.";
    }
}
