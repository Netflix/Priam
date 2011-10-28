package com.priam.netflix.monitoring;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.priam.conf.IConfiguration;
import com.priam.conf.JMXNodeTool;

/**
 * Top level monitor for a node using JMXNodeTool.
 */
public class CassandraMetrics extends AbstractMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraMetrics.class);
    private static final String monitorName = "Cassandra_Info";
    private final Monitors<ColumnFamilyStoreMBean> cfMonitor = new Monitors<ColumnFamilyStoreMBean>()
    {
        @Override
        AbstractMonitor<ColumnFamilyStoreMBean> factory(String name)
        {
            return new ColumnFamilyMonitor(name);
        }
    };
    private final Monitors<JMXEnabledThreadPoolExecutorMBean> tpMonitor = new Monitors<JMXEnabledThreadPoolExecutorMBean>()
    {
        @Override
        AbstractMonitor<JMXEnabledThreadPoolExecutorMBean> factory(String name)
        {
            return new ThreadPoolMonitor(name);
        }
    };

    @Inject
    public CassandraMetrics(IConfiguration config)
    {
        super(config);
        CassandraBasicMonitor basic = new CassandraBasicMonitor(monitorName, config);
        basic.start();
    }

    @Override
    public void execute() throws Exception
    {
        updateTpMonitors();
        updateColumnFamilyMonitors();
    }

    private void updateTpMonitors()
    {
        logger.info("Updating Thread Pool Monitors");
        Iterator<Entry<String, JMXEnabledThreadPoolExecutorMBean>> proxies = JMXNodeTool.instance(config).getThreadPoolMBeanProxies();
        tpMonitor.update(proxies);
    }

    private void updateColumnFamilyMonitors()
    {
        logger.debug("Updating keyspace Monitors");
        Iterator<Entry<String, ColumnFamilyStoreMBean>> proxies = JMXNodeTool.instance(config).getColumnFamilyStoreMBeanProxies();
        cfMonitor.update(proxies);
    }

    @Override
    public String getName()
    {
        return monitorName;
    }
}
