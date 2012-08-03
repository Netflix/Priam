package com.netflix.priam.scheduler;

import com.google.common.base.Throwables;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Task class that should be implemented by all cron tasks. Jobconf will contain
 * any instance specific data
 * 
 * NOTE: Constructor must not throw any exception. This will cause Quartz to set the job to failure
 */
public abstract class Task implements Job, TaskMBean
{
    public STATE status = STATE.DONE;

    public static enum STATE
    {
        ERROR, RUNNING, DONE
    }
    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger executions = new AtomicInteger();

    protected Task()
    {
        this(ManagementFactory.getPlatformMBeanServer());
    }

    protected Task(MBeanServer mBeanServer) {
        // TODO: don't do mbean registration here
        String mbeanName = "com.priam.scheduler:type=" + this.getClass().getName();
        try
        {
            mBeanServer.registerMBean(this, new ObjectName(mbeanName));
            initialize();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    
    /**
     * This method has to be implemented and cannot thow any exception.
     */
    public void initialize() throws ExecutionException
    {
        // nothing to intialize
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
            logger.error("Couldnt execute the task because of " + e.getMessage(), e);
            errors.incrementAndGet();
        }
        catch (Throwable e)
        {
            status = STATE.ERROR;
            logger.error("Couldnt execute the task because of " + e.getMessage(), e);
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
