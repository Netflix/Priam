package com.priam.netflix.monitoring;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.inject.Inject;
import com.priam.conf.IConfiguration;
import com.priam.scheduler.TaskMBean;

/**
 * Send all Tasks metrics to epic + anything which is custome for Priam.
 * 
 * @author "Vijay Parthasarathy"
 */
public class PriamMetrics extends AbstractMetrics
{
    private static final String PRIAM_MONITOR_TASK = "Priam_Monitor";
    Monitors<TaskMBean> taskMonitors = new Monitors<TaskMBean>()
    {
        @Override
        AbstractMonitor<TaskMBean> factory(String name)
        {
            return new TaskMonitor(name);
        }
    };

    @Inject
    public PriamMetrics(IConfiguration config)
    {
        super(config);
        FileSystemMonitor fsm = new FileSystemMonitor("Priam");
        fsm.start();
    }

    @Override
    public void execute() throws Exception
    {
        taskMonitors.update(new TaskMBeanIterator());
    }

    @Override
    public String getName()
    {
        return PRIAM_MONITOR_TASK;
    }

    class TaskMBeanIterator implements Iterator<Map.Entry<String, TaskMBean>>
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        private Iterator<ObjectName> resIter;

        public TaskMBeanIterator() throws MalformedObjectNameException, NullPointerException, IOException
        {
            ObjectName query = new ObjectName("com.priam.scheduler:type=*");
            resIter = mbs.queryNames(query, null).iterator();
        }

        public boolean hasNext()
        {
            return resIter.hasNext();
        }

        public Entry<String, TaskMBean> next()
        {
            ObjectName objectName = resIter.next();
            TaskMBean mbean = (TaskMBean) MBeanServerInvocationHandler.newProxyInstance(mbs, objectName, TaskMBean.class, false);
            return new AbstractMap.SimpleImmutableEntry<String, TaskMBean>(objectName.getCanonicalName(), mbean);
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
