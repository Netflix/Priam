package com.netflix.priam.agent.commands;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.utils.JMXNodeTool;

public class CommandMove implements AgentProcess
{
    @Override
    public void performCommand(IConfiguration configuration, String[] arguments) throws Exception
    {
        JMXNodeTool     nodeTool = JMXNodeTool.instance(configuration);
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
