package com.netflix.priam.scheduler;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.IConfiguration;

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
    protected final IConfiguration config;
    
    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger executions = new AtomicInteger();

    public Task(IConfiguration config)
    {
        this.config = config;
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = "com.priam.scheduler:type=" + this.getClass().getName();
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
            initialize();
        }
        catch (Exception e)
        {
            logger.error("Error executing the task: ", e);
            System.exit(100);
        }
        catch (Throwable th)
        {
            logger.error("Error executing the task: ", th);
            System.exit(100);
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
