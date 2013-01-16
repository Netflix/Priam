package com.netflix.priam.agent.process;

import com.netflix.priam.agent.NodeStatus;

public interface AgentProcess
{
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception;

    public String getHelpText();
}
