package com.netflix.priam.scheduler;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.TestModule;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

public class TestScheduler
{

    private  static boolean setToFalse;
    
    @Test
    public void testSchedule() throws Exception
    {
        setToFalse = true;
        Injector inject = Guice.createInjector(new TestModule());
        PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
        scheduler.start();
        scheduler.addTask("test", TestTask.class, new SimpleTimer("testtask", 10000));
        Thread.sleep(1000);
        scheduler.shutdown();
        Assert.assertEquals(false, setToFalse);
    }

    @Test
    public void testSingleInstanceSchedule() throws Exception
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
        @Inject
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
        @Inject
        public SingleTestTask(IConfiguration config)
        {
            super(config);
        }

        public static int count =0;
        @Override
        public void execute()
        {
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
