package com.netflix.priam.agent.commands;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.utils.JMXNodeTool;

public class CommandFlush implements AgentProcess
{
    @Override
    public void performCommand(IConfiguration configuration, String[] arguments) throws Exception
    {
        JMXNodeTool     nodeTool = JMXNodeTool.instance(configuration);
        nodeTool.flush();
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.flush()";
    }
}
