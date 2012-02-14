package com.priam.scheduler;

import java.util.Date;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.priam.TestModule;

public class TestScheduler
{

    private  static boolean setToFalse;
    
    @Test
    public void testScedule() throws Exception
    {
        setToFalse = true;
        Injector inject = Guice.createInjector(new TestModule());
        PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
        scheduler.start();
        scheduler.addTask("test", TestTask.class, new SimpleTimer("testtask", 100));
        scheduler.addTask("test1", TestTask.class, new SimpleTimer("test1"));
        Thread.sleep(10000);
        scheduler.shutdown();
        Assert.assertEquals(false, setToFalse);
    }

    @Test
    public void testSingleInstanceScedule() throws Exception
    {
        Injector inject = Guice.createInjector(new TestModule());
        PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
        scheduler.start();
        scheduler.addTask("test2", SingleTestTask.class, SingleTestTask.getTimer());
        Thread.sleep(15*1000);
        scheduler.shutdown();
        Assert.assertEquals(3, SingleTestTask.count);
    }

    @Ignore
    public static class TestTask extends Task
    {
        public TestTask(IConfiguration config)
        {
            super(config);
        }

        @Override
        public void execute()
        {
            setToFalse = false;
        }

        @Override
        public String getName()
        {
            return "test";
        }

    }
    
    @Ignore
    @Singleton
    public static class SingleTestTask extends Task
    {
        public SingleTestTask(IConfiguration config)
        {
            super(config);
        }

        public static int count =0;
        @Override
        public void execute()
        {
            System.out.println( new Date() + " Running ");
            ++count;
            try
            {
                Thread.sleep(5000);//5sec
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        @Override
        public String getName()
        {
            return "test2";
        }
        
        public static TaskTimer getTimer()
        {
            return new SimpleTimer("test2", 1000L);
        }


    }

}
