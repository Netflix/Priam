package com.netflix.cassandra.token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.library.NFLibraryException;
import com.netflix.library.NFLibraryManager;
import com.netflix.platform.core.PlatformManager;
import com.priam.backup.BRTestModule;
import com.priam.identity.PriamInstance;
import com.priam.netflix.InstanceDataDAOCassandra;

public class TestCassInstanceDAO
{
    private static Injector injector;
    
    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        Properties props = new Properties();
        
        props.setProperty("platform.ListOfComponentsToInit", "LOGGING,APPINFO,DISCOVERY");
     
        System.setProperty("netflix.environment", "test");
        System.setProperty("netflix.logging.realtimetracers", "true");
        System.setProperty("netflix.appinfo.name", "nfcassandra.unittest");
        
        try {
            NFLibraryManager.initLibrary(PlatformManager.class, props, true, false);
        } catch (NFLibraryException e) {
            Assert.fail();
        }
    }

    @AfterClass
    public static void cleanup() throws Exception
    {
        InstanceDataDAOCassandra dao = injector.getInstance(InstanceDataDAOCassandra.class);
        PriamInstance ins = new PriamInstance();
        ins.setApp("MyTestAppCre");
        ins.setDC("us-east-1");
        ins.setHost("testhost", "ec2-test2-host.com");
        ins.setId(1);
        ins.setInstanceId("i-abcdef81");
        ins.setPayload("123456789");
        ins.setRac("us-east-1a");
        dao.deleteInstanceEntry(ins);

    }
    
    @Test
    public void testCreate() throws Exception{
        InstanceDataDAOCassandra dao = injector.getInstance(InstanceDataDAOCassandra.class);
        PriamInstance ins = new PriamInstance();
        ins.setApp("MyTestAppCre");
        ins.setDC("us-east-1");
        ins.setHost("testhost", "ec2-test2-host.com");
        ins.setId(1);
        ins.setInstanceId("i-abcdef81");
        ins.setPayload("123456789");
        ins.setRac("us-east-1a");
        dao.createInstanceEntry(ins);
        Assert.assertEquals(dao.getAllInstances("MyTestAppCre").size(), 1);
    }

    @Test
    public void testDelete() throws Exception{
        InstanceDataDAOCassandra dao = injector.getInstance(InstanceDataDAOCassandra.class);
        PriamInstance ins = new PriamInstance();
        ins.setApp("MyTestAppDel");
        ins.setDC("us-east-1");
        ins.setHost("testhost", "ec2-test2-host.com");
        ins.setId(1);
        ins.setInstanceId("i-abcdef81");
        ins.setPayload("123456789");
        ins.setRac("us-east-1a");
        dao.createInstanceEntry(ins);
        PriamInstance ins1 = dao.getInstance("MyTestAppDel", 1);
        Assert.assertNotNull(ins1);
        dao.deleteInstanceEntry(ins);
        ins1 = dao.getInstance("MyTestAppDel", 1);
        Assert.assertNull(ins1);        
    }

    @Test
    public  void testCreateDelete() throws Exception{
        InstanceDataDAOCassandra dao = injector.getInstance(InstanceDataDAOCassandra.class);
        PriamInstance ins = new PriamInstance();
        ins.setApp("MyTestAppCre");
        ins.setDC("us-east-1");
        ins.setHost("testhost", "ec2-test2-host.com");
        ins.setId(1);
        ins.setInstanceId("i-abcdef81");
        ins.setPayload("123456789");
        ins.setRac("us-east-1a");
        dao.deleteInstanceEntry(ins);
        dao.createInstanceEntry(ins);
        Assert.assertEquals(dao.getAllInstances("MyTestAppCre").size(), 1);
        dao.deleteInstanceEntry(ins);
        Assert.assertEquals(dao.getAllInstances("MyTestAppCre").size(), 0);
    }

    @Test
    public void testDeleteAppEntries() throws Exception
    {
        InstanceDataDAOCassandra dao = injector.getInstance(InstanceDataDAOCassandra.class);
        for( PriamInstance ins : dao.getAllInstances("MyTestApp")){
            dao.deleteInstanceEntry(ins);
        }
    }

    //@Test
    public void testParallelCreate() throws Exception{
        InstanceDataDAOCassandra dao = injector.getInstance(InstanceDataDAOCassandra.class);
        PriamInstance instance = new PriamInstance();
        instance.setApp("MyTestApp2");
        instance.setDC("us-east-1");
        instance.setHost("testhost", "ec2-test2-host.com");
        instance.setId(1);
        instance.setInstanceId("i-abcdef811");
        instance.setPayload("123456789");
        instance.setRac("us-east-1a");
        dao.deleteInstanceEntry(instance);
        int tcount = 25;
        List<TestPriamThread> threads = new ArrayList<TestPriamThread>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(tcount);
        
        for (int i = 0; i < tcount; ++i){
            TestPriamThread prthread = new TestPriamThread(startLatch, doneLatch, Integer.toHexString(i));
            threads.add(prthread);
            new Thread(prthread).start();
        }

        startLatch.countDown(); 
        while( doneLatch.getCount() >= tcount){
            Thread.sleep(1000);
        }
        //doneLatch.await();
        Thread.sleep(60000);
        int successcount = 0;
        for( TestPriamThread t : threads)
            successcount += (t.wasSuccessful?1:0);
        
        Assert.assertEquals(1, successcount);
    }

    class TestPriamThread implements Runnable
    {
        private final CountDownLatch startLatch;
        private final CountDownLatch doneLatch;
        private final PriamInstance instance;
        public boolean wasSuccessful = true;

        TestPriamThread(CountDownLatch startLatch, CountDownLatch doneLatch, String id) {
            this.startLatch = startLatch;
            this.doneLatch = doneLatch;
            instance = new PriamInstance();
            instance.setApp("MyTestApp2");
            instance.setDC("us-east-1");
            instance.setHost("testhost", "ec2-test2-host.com");
            instance.setId(1);
            instance.setInstanceId("i-abcdef81" + id);
            instance.setPayload("123456789");
            instance.setRac("us-east-1a");
        }

        public void run()
        {
            try {
                startLatch.await();
                InstanceDataDAOCassandra dao = injector.getInstance(InstanceDataDAOCassandra.class);
                while (true) {
                    try {
                        dao.createInstanceEntry(instance);
                        System.out.println("Got lock " + instance.getInstanceId());
                        wasSuccessful = true;
                        break;
                    }
                    catch (Exception e) {
                        wasSuccessful = false;
                        System.out.println("Couldnt get lock " + instance.app + " " + instance.instanceId + " " + e.getMessage());
                        Thread.sleep(10000 + new Random().nextInt(15000));
                    }
                }
                doneLatch.countDown();
            }
            catch (InterruptedException ex) {
            } // return;
        }

    }
}
