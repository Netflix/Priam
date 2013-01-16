package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;

public class CommandRemoveNode implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        if ( arguments.length == 0 )
        {
            throw new IllegalStateException("a token argument must be provided");
        }
        nodeTool.removeNode(arguments[0]);
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.removeNode(token). Argument is the token.";
    }
}
