package com.priam.scheduler;

import org.junit.Ignore;
import org.junit.Test;
import org.quartz.JobDataMap;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.logging.LogManager;
import com.priam.TestModule;
import com.priam.conf.IConfiguration;

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
        Thread.sleep(100);
        scheduler.shutdown();
    }

    @Ignore
    public static class TestTask extends Task
    {
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
}
