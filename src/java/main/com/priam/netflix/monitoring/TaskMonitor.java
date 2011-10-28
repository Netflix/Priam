package com.priam.netflix.monitoring;

import com.netflix.monitoring.DataSourceType;
import com.netflix.monitoring.Monitor;
import com.priam.scheduler.TaskMBean;

/**
 * This will add the counters on the TaskMbean
 * 
 * @author "Vijay Parthasarathy"
 */
public class TaskMonitor extends AbstractMonitor<TaskMBean>
{
    public TaskMonitor(String name)
    {
        super("Task_" + name);
    }

    @Monitor(dataSourceName = "ErrorCount", type = DataSourceType.COUNTER)
    public int getErrorCount()
    {
        return bean.getErrorCount();
    }

    @Monitor(dataSourceName = "ExecutionCount", type = DataSourceType.COUNTER)
    public int getExecutionCount()
    {
        return bean.getErrorCount();
    }
}
