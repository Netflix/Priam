package com.priam.netflix.monitoring;

import java.util.Map;

import com.netflix.monitoring.DataSourceType;
import com.netflix.monitoring.Monitor;
import com.priam.conf.IConfiguration;
import com.priam.conf.JMXNodeTool;

/**
 * Basic Monitoring for cassandra
 * 
 * @author "Vijay Parthasarathy"
 */
public class CassandraBasicMonitor extends AbstractMonitor
{
    private IConfiguration config;

    public CassandraBasicMonitor(String name, IConfiguration config)
    {
        super(name);
        this.config = config;
    }

    @Monitor(dataSourceName = "ExceptionCount", type = DataSourceType.COUNTER)
    public long getExceptionCount()
    {
        return JMXNodeTool.instance(config).getExceptionCount();
    }

    @Monitor(dataSourceName = "DroppedTotalMessageCount", type = DataSourceType.COUNTER)
    public long getTotalDroppedMessages()
    {
        long count = 0;
        for (Map.Entry<String, Integer> entry : JMXNodeTool.instance(config).getDroppedMessages().entrySet())
        {
            count += entry.getValue();
        }
        return count;
    }

    @Monitor(dataSourceName = "Compactions_Completed", type = DataSourceType.COUNTER)
    public long getCompactionsCompleted()
    {
        return JMXNodeTool.instance(config).getCompactionManagerProxy().getCompletedTasks();
    }

    @Monitor(dataSourceName = "Compactions_Pending", type = DataSourceType.GAUGE)
    public long getCompactionsPending()
    {
        return JMXNodeTool.instance(config).getCompactionManagerProxy().getPendingTasks();
    }
}
