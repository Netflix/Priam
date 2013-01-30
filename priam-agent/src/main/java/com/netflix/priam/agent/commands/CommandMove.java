package com.netflix.priam.agent.commands;

import com.google.common.collect.Lists;
import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ArgumentMetaData;
import com.netflix.priam.agent.process.ProcessMetaData;
import java.util.List;

public class CommandMove implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        nodeTool.move(arguments[0]);
    }

    @Override
    public ProcessMetaData getMetaData()
    {
        return new ProcessMetaData()
        {
            @Override
            public String getHelpText()
            {
                return "Calls nodeTool.move(token). Argument is the token to move.";
            }

            @Override
            public int getMinArguments()
            {
                return 1;
            }
        };
    }
}
