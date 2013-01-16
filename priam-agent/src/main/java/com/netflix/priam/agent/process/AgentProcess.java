package com.netflix.priam.agent.process;

import com.netflix.priam.IConfiguration;

public interface AgentProcess
{
    public void performCommand(IConfiguration configuration, String[] arguments) throws Exception;

    public String getHelpText();
}
