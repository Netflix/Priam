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
                return "Calls nodeTool.move(token)";
            }

            @Override
            public List<ArgumentMetaData> getArguments()
            {
                ArgumentMetaData token = new ArgumentMetaData()
                {
                    @Override
                    public String getName()
                    {
                        return "token";
                    }

                    @Override
                    public String getDescription()
                    {
                        return "The token to move";
                    }

                    @Override
                    public boolean isVariableLength()
                    {
                        return false;
                    }

                    @Override
                    public boolean isOptional()
                    {
                        return false;
                    }
                };
                return Lists.newArrayList(token);
            }
        };
    }
}
