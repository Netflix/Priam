package com.netflix.priam.agent.commands;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.utils.JMXNodeTool;
import java.util.Arrays;

public class CommandRefresh implements AgentProcess
{
    @Override
    public void performCommand(IConfiguration configuration, String[] arguments) throws Exception
    {
        JMXNodeTool     nodeTool = JMXNodeTool.instance(configuration);
        nodeTool.refresh(Arrays.asList(arguments));
    }

    @Override
    public String getHelpText()
    {
        return "Calls nodeTool.refresh(keyspaces). Arguments is an array of keyspaces";
    }
}
