package com.netflix.priam.scheduler;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.netflix.priam.TestModule;
import com.netflix.priam.backup.Restore;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;

import javax.management.MBeanServerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestScheduler
{
    // yuck, but marginally better than using Thread.sleep
    private static CountDownLatch latch;

    @Test
    public void testSchedule() throws Exception
    {
        latch = new CountDownLatch(1);
        Injector inject = Guice.createInjector(new TestModule());
        PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
        scheduler.start();
        scheduler.addTask(TestTask.getJobDetail(), new SimpleTimer("testtask", 10).getTrigger());
        // verify the task has run or fail in 1s
        latch.await(1000, TimeUnit.MILLISECONDS);
        scheduler.shutdown();
    }

    @Test
    public void testSingleInstanceSchedule() throws Exception
    {
        latch = new CountDownLatch(3);
        Injector inject = Guice.createInjector(new TestModule());
        PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
        scheduler.start();
        scheduler.addTask(SingleTestTask.getJobDetail(), new SimpleTimer("testSingleInstance", 11L).getTrigger());
        // verify 3 tasks run or fail in 1s
        latch.await(1000, TimeUnit.MILLISECONDS);
        scheduler.shutdown();
        Assert.assertEquals(3, SingleTestTask.count);
    }

    @Ignore
    public static class TestTask extends Task
    {
        @Inject
        public TestTask()
        {
            // todo: mock the MBeanServer instead, but this will prevent exceptions due to duplicate registrations
            super(MBeanServerFactory.newMBeanServer());
        }

        @Override
        public void execute()
        {
            latch.countDown();
        }

        @Override
        public String getName()
        {
            return "test";
        }

        public static JobDetail getJobDetail(){
            JobDetail jobDetail = JobBuilder.newJob(TestTask.class)
                    .withIdentity("priam-scheduler", "testTask")
                    .build();
            return jobDetail;
        }

    }

    @Ignore
    @Singleton
    public static class SingleTestTask extends Task
    {
        @Inject
        public SingleTestTask()
        {
            super(MBeanServerFactory.newMBeanServer());
        }

        public static volatile int count =0;
        @Override
        public void execute()
        {
            ++count;
            latch.countDown();
            try
            {
                // todo : why is this sleep important?
                Thread.sleep(55);//5sec
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
            return new SimpleTimer("test2", 11L);
        }

        public static JobDetail getJobDetail(){
            JobDetail jobDetail = JobBuilder.newJob(TestTask.class)
                    .withIdentity("priam-scheduler", "singleTestTask")
                    .build();
            return jobDetail;
        }
    }
}
