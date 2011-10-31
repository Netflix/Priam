package com.priam.scheduler;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task class that should be implemented by all cron tasks. Jobconf will contain
 * any instance specific data
 */
public abstract class Task implements Job, TaskMBean
{
    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger executions = new AtomicInteger();
    public STATE status = STATE.DONE;
    public static enum STATE
    {
        ERROR, RUNNING, DONE
    }
    
    public Task()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = "com.priam.scheduler:type=" + this.getClass().getName();
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public abstract void execute() throws Exception;

    /**
     * Main method to execute a task
     */
    public void execute(JobExecutionContext context) throws JobExecutionException
    {
        executions.incrementAndGet();
        try
        {
            if (status == STATE.RUNNING)
                return;
            status = STATE.RUNNING;
            execute();

        }
        catch (Exception e)
        {
            status = STATE.ERROR;
            logger.error("Couldnt execute the task because of....", e);
            errors.incrementAndGet();
        }
        catch (Throwable e)
        {
            status = STATE.ERROR;
            logger.error("Couldnt execute the task because of....", e);
            errors.incrementAndGet();
        }
        if (status != STATE.ERROR)
            status = STATE.DONE;
    }

    public STATE state()
    {
        return status;
    }
    
    public int getErrorCount()
    {
        return errors.get();
    }
    
    public int getExecutionCount()
    {
        return executions.get();
    }

    public abstract String getName();

}
