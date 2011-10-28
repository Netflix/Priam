package com.priam.netflix.monitoring;

import com.netflix.monitoring.MonitorId;
import com.netflix.monitoring.MonitorRegistry;

public abstract class AbstractMonitor<T>
{
    @MonitorId
    final String name;
    volatile T bean;

    public AbstractMonitor(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }
    
    public AbstractMonitor<T> setBean(T bean)
    {
        this.bean = bean;
        return this;
    }

    public void start()
    {
        MonitorRegistry.getInstance().registerObject(this);
    }

    public void shutdown()
    {
        MonitorRegistry.getInstance().unRegisterObject(this);
    }
}
