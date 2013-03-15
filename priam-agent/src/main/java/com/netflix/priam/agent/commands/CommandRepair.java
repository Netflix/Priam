package com.netflix.priam.agent.commands;

import com.google.common.collect.Sets;
import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.ProcessMetaData;
import com.netflix.priam.agent.process.SimpleProcessMetaData;
import java.util.Arrays;
import java.util.Set;

public class CommandRepair implements AgentProcess
{
    @Override
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception
    {
        Set<String>     argumentsSet = Sets.newHashSet(Arrays.asList(arguments));
        boolean         sequential = argumentsSet.contains("sequential");
        nodeTool.repair(sequential);
    }

    @Override
    public ProcessMetaData getMetaData()
    {
        return new SimpleProcessMetaData("Calls nodeTool.repair(sequential, localDataCenterOnly). If arguments contains \"sequential\", sequential is set to true. localDataCenterOnly is true if arguments contains \"local-dc-only\".");
    }
}
