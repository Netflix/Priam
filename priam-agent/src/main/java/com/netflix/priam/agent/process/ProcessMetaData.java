package com.netflix.priam.agent.process;

/**
 * Meta data about a process
 */
public interface ProcessMetaData
{
    /**
     * @return user displayable description of the process and any arguments
     */
    public String getHelpText();

    /**
     * @return minimum number of arguments the process requires or 0
     */
    public int getMinArguments();
}
