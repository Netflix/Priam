package com.priam.netflix.monitoring;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.priam.conf.IConfiguration;
import com.priam.scheduler.Task;

public abstract class AbstractMetrics extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMetrics.class);
    public final IConfiguration config;

    public AbstractMetrics(IConfiguration config)
    {
        this.config = config;
    }

    public static abstract class Monitors<T>
    {
        final ConcurrentHashMap<String, AbstractMonitor<T>> monitors = new ConcurrentHashMap<String, AbstractMonitor<T>>();

        abstract AbstractMonitor<T> factory(String name);

        public void update(Iterator<Entry<String, T>> updates)
        {
            Set<String> toRemove = new HashSet<String>(monitors.keySet());
            while (updates.hasNext())
            {
                Entry<String, T> proxy = updates.next();
                AbstractMonitor<T> newMonitor = factory(proxy.getKey());
                AbstractMonitor<T> prevMonitor = monitors.putIfAbsent(proxy.getKey(), newMonitor);
                if (prevMonitor == null)
                {
                    logger.info("Starting thread pool monitor: " + newMonitor.getName());
                    newMonitor.start();
                }
                else
                {
                    prevMonitor.setBean(proxy.getValue());
                }
                toRemove.remove(proxy.getKey());
            }

            for (String key : toRemove)
            {
                logger.info("Removing thread pool monitor: " + key);
                AbstractMonitor<T> prevMonitor = monitors.remove(key);
                if (prevMonitor != null)
                    prevMonitor.shutdown();
            }
        }
    }
}
