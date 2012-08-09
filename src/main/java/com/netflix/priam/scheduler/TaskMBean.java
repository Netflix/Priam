package com.netflix.priam.scheduler;

/**
 * MBean to monitor Task executions.
 */
public interface TaskMBean {
    public int getErrorCount();

    public int getExecutionCount();

    public String getName();
}
