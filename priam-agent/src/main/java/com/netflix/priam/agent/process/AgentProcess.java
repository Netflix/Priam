package com.netflix.priam.agent.process;

import com.netflix.priam.agent.NodeStatus;

/**
 * Command interface for a process
 */
public interface AgentProcess
{
    /**
     * Perform the command and return when completed
     *
     * @param nodeTool the node tool instance
     * @param arguments any arguments
     * @throws Exception errors
     */
    public void performCommand(NodeStatus nodeTool, String[] arguments) throws Exception;

    /**
     * Return user displayable metadata about the process
     *
     * @return metadata
     */
    public ProcessMetaData getMetaData();
}
