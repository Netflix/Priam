package com.netflix.priam.agent.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ArgumentMetaData;
import com.netflix.priam.agent.process.ProcessMetaData;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class CommandRepair implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        Set<String>     argumentsSet = Sets.newHashSet(Arrays.asList(arguments));
        boolean         sequential = argumentsSet.contains("sequential");
        boolean         localDataCenterOnly = argumentsSet.contains("local-dc-only");
        nodeTool.repair(sequential, localDataCenterOnly);
    }

    @Override
    public ProcessMetaData getMetaData()
    {
        return new ProcessMetaData()
        {
            @Override
            public String getHelpText()
            {
                return "Calls nodeTool.repair(sequential, localDataCenterOnly)";
            }

            @Override
            public List<ArgumentMetaData> getArguments()
            {
                ArgumentMetaData options = new ArgumentMetaData()
                {
                    @Override
                    public String getName()
                    {
                        return "options";
                    }

                    @Override
                    public String getDescription()
                    {
                        return "If options contains \"sequential\", sequential is set to true. localDataCenterOnly is true if options contains \"local-dc-only\"";
                    }

                    @Override
                    public boolean isVariableLength()
                    {
                        return true;
                    }

                    @Override
                    public boolean isOptional()
                    {
                        return true;
                    }
                };
                return Lists.newArrayList(options);
            }
        };
    }
}
