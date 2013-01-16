package com.netflix.priam.agent.commands;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.utils.JMXNodeTool;

public class CommandStopGossiping implements AgentProcess
{
    @Override
    public void performCommand(IConfiguration configuration, String[] arguments) throws Exception
    {
        JMXNodeTool     nodeTool = JMXNodeTool.instance(configuration);
        nodeTool.stopGossiping();
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.stopGossiping()";
    }
}
