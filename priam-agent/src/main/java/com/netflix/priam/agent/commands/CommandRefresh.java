package com.netflix.priam.agent.commands;

import com.google.common.collect.Lists;
import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ArgumentMetaData;
import com.netflix.priam.agent.process.ProcessMetaData;
import java.util.Arrays;
import java.util.List;

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
                return "Calls nodeTool.refresh(keyspaces)";
            }

            @Override
            public List<ArgumentMetaData> getArguments()
            {
                ArgumentMetaData        metaData = new ArgumentMetaData()
                {
                    @Override
                    public String getName()
                    {
                        return "keyspaces";
                    }

                    @Override
                    public String getDescription()
                    {
                        return "list of keyspace names";
                    }

                    @Override
                    public boolean isVariableLength()
                    {
                        return true;
                    }

                    @Override
                    public boolean isOptional()
                    {
                        return false;
                    }
                };
                return Lists.newArrayList(metaData);
            }
        };
    }
}
