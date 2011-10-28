package com.priam.netflix.monitoring;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;

import com.netflix.monitoring.DataSourceType;
import com.netflix.monitoring.Monitor;

/**
 * Monitor a single Column Family
 */
public class ColumnFamilyMonitor extends AbstractMonitor<ColumnFamilyStoreMBean>
{
    public ColumnFamilyMonitor(String name)
    {
        super("ColumnFamily_" + name);
    }

    @Monitor(dataSourceName = "WriteCount", type = DataSourceType.COUNTER)
    public long getWriteCount()
    {
        return bean.getWriteCount();
    }

    @Monitor(dataSourceName = "ReadCount", type = DataSourceType.COUNTER)
    public long getReadCount()
    {
        return bean.getReadCount();
    }

    @Monitor(dataSourceName = "ReadLatency", type = DataSourceType.COUNTER)
    public double getTotalReadLatency()
    {
        return bean.getTotalReadLatencyMicros();
    }

    @Monitor(dataSourceName = "WriteLatency", type = DataSourceType.COUNTER)
    public double getTotalWriteLatency()
    {
        return bean.getTotalWriteLatencyMicros();
    }
}
