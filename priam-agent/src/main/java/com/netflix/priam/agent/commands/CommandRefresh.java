package com.netflix.priam.agent.commands;

import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ProcessMetaData;
import java.util.Arrays;

public class CommandRefresh implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.refresh(Arrays.asList(arguments));
    }

    @Override
    public ProcessMetaData getMetaData()
    {
        return new ProcessMetaData()
        {
            @Override
            public String getHelpText()
            {
                return "Calls nodeTool.refresh(keyspaces). Each argument is a keyspace to refresh.";
            }

            @Override
            public int getMinArguments()
            {
                return 1;
            }
        };
    }
}
