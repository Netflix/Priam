package com.priam.scheduler;

/**
 * MBean to monitor Task executions.
 * 
 * @author "Vijay Parthasarathy"
 */
public interface TaskMBean
{
    public int getErrorCount();
    public int getExecutionCount();
    public String getName();
}
