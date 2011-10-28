package com.priam.netflix.monitoring;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;

import com.netflix.monitoring.DataSourceType;
import com.netflix.monitoring.Monitor;

/**
 * Monitor for a single thread pool
 */
public class ThreadPoolMonitor extends AbstractMonitor<JMXEnabledThreadPoolExecutorMBean>
{
    public ThreadPoolMonitor(String name)
    {
        super("ThreadPool_" + name);
    }

    @Monitor(dataSourceName = "Completed", type = DataSourceType.COUNTER)
    public long getCompleted()
    {
        return bean.getCompletedTasks();
    }

    @Monitor(dataSourceName = "Blocked", type = DataSourceType.GAUGE)
    public long getBlocked()
    {
        return bean.getCurrentlyBlockedTasks();
    }

    @Monitor(dataSourceName = "Pending", type = DataSourceType.GAUGE)
    public long getPending()
    {
        return bean.getPendingTasks();
    }

    @Monitor(dataSourceName = "Active", type = DataSourceType.GAUGE)
    public long getActive()
    {
        return bean.getActiveCount();
    }

}
