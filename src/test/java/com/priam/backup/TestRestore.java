package com.priam.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ibm.icu.util.Calendar;
import com.priam.conf.IConfiguration;
import com.priam.identity.IPriamInstanceFactory;

public class TestRestore
{
    private static Injector injector;
    private static FakeBackupFileSystem filesystem;
    private static ArrayList<String> fileList;
    private static Calendar cal;
    
    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        filesystem = (FakeBackupFileSystem)injector.getInstance(IBackupFileSystem.class);
        fileList = new ArrayList<String>();
        cal = Calendar.getInstance();
    }
    
    public static void populateBackupFileSystem(){
        fileList.clear();
        fileList.add("test_backup/fake-region/fakecluster/123456/201108110030/META/meta.json");
        fileList.add("test_backup/fake-region/fakecluster/123456/201108110030/SNAP/ks1/f1.db");
        fileList.add("test_backup/fake-region/fakecluster/123456/201108110030/SNAP/ks1/f2.db");
        fileList.add("test_backup/fake-region/fakecluster/123456/201108110030/SNAP/ks2/f2.db");
        fileList.add("test_backup/fake-region/fakecluster/123456/201108110530/SST/ks2/f3.db");
        fileList.add("test_backup/fake-region/fakecluster/123456/201108110600/SST/ks2/f4.db");
        filesystem.setupTest(fileList);
    }
    
    @Test
    public void testRestore() throws Exception 
    {
        populateBackupFileSystem();
        Restore restore = injector.getInstance(Restore.class);
        cal.set(2011, 7, 11, 0, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date startTime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        restore.restore(startTime, cal.getTime());
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
    }

    //Pick latest file
    @Test 
    public void testRestoreLatest() throws Exception 
    {
        populateBackupFileSystem();
        String metafile = "test_backup/fake-region/fakecluster/123456/201108110130/META/meta.json";
        filesystem.addFile(metafile);
        Restore restore = injector.getInstance(Restore.class);
        cal.set(2011, 7, 11, 0, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date startTime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        restore.restore(startTime, cal.getTime());
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(metafile));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
    }

    @Test
    public void testNoSnapshots() throws Exception 
    {
        try {        
            filesystem.setupTest(new ArrayList<String>());
            Restore restore = new Restore(injector.getInstance(IConfiguration.class), injector.getInstance(IPriamInstanceFactory.class), filesystem);            
            cal.set(2011, 8, 11, 0, 30);
            Date startTime = cal.getTime();
            cal.add(Calendar.HOUR, 5);
            restore.restore(startTime, cal.getTime());
            Assert.assertFalse(true);//No exception thrown
        }
        catch (AssertionError e) {
            Assert.assertEquals("[cass_backup] No snapshots found, Restore Failed.", e.getMessage());
        }
    }

}
