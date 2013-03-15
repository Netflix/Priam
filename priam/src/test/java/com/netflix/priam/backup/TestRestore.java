package com.netflix.priam.backup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.IConfiguration;

public class TestRestore
{
    private static Injector injector;
    private static FakeBackupFileSystem filesystem;
    private static ArrayList<String> fileList;
    private static Calendar cal;    
    private static IConfiguration conf;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        filesystem = (FakeBackupFileSystem)injector.getInstance(IBackupFileSystem.class);
        conf = injector.getInstance(IConfiguration.class);
        fileList = new ArrayList<String>();
        File cassdir = new File(conf.getDataFileLocation());
        cassdir.mkdirs();
        cal = Calendar.getInstance();
    }

    @AfterClass
    public static void cleanup() throws IOException
    {
        File file = new File("cass");
        FileUtils.deleteQuietly(file);
    }

    public static void populateBackupFileSystem(String baseDir){
        fileList.clear();
        fileList.add(baseDir + "/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/META/meta.json");
        fileList.add(baseDir + "/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db");
        fileList.add(baseDir + "/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks1/cf1/f2.db");
        fileList.add(baseDir + "/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks2/cf1/f2.db");
        fileList.add(baseDir + "/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110530/SST/ks2/cf1/f3.db");
        fileList.add(baseDir + "/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks2/cf1/f4.db");
        filesystem.baseDir = baseDir;
        filesystem.region = FakeConfiguration.FAKE_REGION;
        filesystem.clusterName = "fakecluster";
        filesystem.setupTest(fileList);
    }

    @Test
    public void testRestore() throws Exception
    {
        populateBackupFileSystem("test_backup");
        File tmpdir = new File(conf.getDataFileLocation() + "/test");
        tmpdir.mkdir();
        Assert.assertTrue(tmpdir.exists());
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
        tmpdir = new File(conf.getDataFileLocation() + "/test");
        Assert.assertFalse(tmpdir.exists());
    }

    //Pick latest file
    @Test 
    public void testRestoreLatest() throws Exception
    {
        populateBackupFileSystem("test_backup");
        String metafile = "test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110130/META/meta.json";
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
            Restore restore = injector.getInstance(Restore.class);
            cal.set(2011, 8, 11, 0, 30);
            Date startTime = cal.getTime();
            cal.add(Calendar.HOUR, 5);
            restore.restore(startTime, cal.getTime());
            Assert.assertFalse(true);//No exception thrown
        }
        catch(java.lang.ArrayIndexOutOfBoundsException e){
            //We are ok
        }catch(java.util.NoSuchElementException e){
            //We are ok
        }
        catch (AssertionError e) {
            Assert.assertEquals("[cass_backup] No snapshots found, Restore Failed.", e.getMessage());
        }
    }
    
    
    @Test
    public void testRestoreFromDiffCluster() throws Exception 
    {
        populateBackupFileSystem("test_backup_new");
        FakeConfiguration conf = (FakeConfiguration)injector.getInstance(IConfiguration.class);
        conf.setRestorePrefix("RESTOREBUCKET/test_backup_new/"+FakeConfiguration.FAKE_REGION+"/fakecluster");
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
        conf.setRestorePrefix("");
    }
}
