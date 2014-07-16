package com.netflix.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;
import mockit.Mock;
import mockit.Mockit;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.netflix.priam.utils.CassandraMonitor;

/**
 * Unit test case to test a snapshot backup and incremental backup
 * 
 * @author Praveen Sadhu
 * 
 */
public class TestBackup
{
    private static Injector injector;
    private static FakeBackupFileSystem filesystem;
    private static final Logger logger = LoggerFactory.getLogger(TestBackup.class);
    private static Set<String> expectedFiles = new HashSet<String>();

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        filesystem = (FakeBackupFileSystem) injector.getInstance(Key.get(IBackupFileSystem.class,Names.named("backup")));
        Mockit.setUpMock(NodeProbe.class, MockNodeProbe.class);
    }
    
    @AfterClass
    public static void cleanup() throws IOException
    {
        File file = new File("target/data");
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testSnapshotBackup() throws Exception
    {
    		filesystem.setupTest();
        SnapshotBackup backup = injector.getInstance(SnapshotBackup.class);
        CassandraMonitor cassMon = injector.getInstance(CassandraMonitor.class);
        cassMon.setIsCassandraStarted();
        backup.execute();
        Assert.assertEquals(3, filesystem.uploadedFiles.size());
        System.out.println("***** "+filesystem.uploadedFiles.size());
        boolean metafile = false;
        for (String filePath : expectedFiles)
            Assert.assertTrue(filesystem.uploadedFiles.contains(filePath));
            
        for(String filepath : filesystem.uploadedFiles){
            if( filepath.endsWith("meta.json")){             
                metafile = true;
                break;
            }
        }        
        Assert.assertTrue(metafile);
    }

    @Test
    public void testIncrementalBackup() throws Exception
    {
    		filesystem.setupTest();
        generateIncrementalFiles();
        IncrementalBackup backup = injector.getInstance(IncrementalBackup.class);
        backup.execute();
        Assert.assertEquals(4, filesystem.uploadedFiles.size());
        for (String filePath : expectedFiles)
            Assert.assertTrue(filesystem.uploadedFiles.contains(filePath));
    }

    public static void generateIncrementalFiles()
    {
        File tmp = new File("target/data/");
        if (tmp.exists())
            cleanup(tmp);
        // Setup
        Set<String> files = new HashSet<String>();
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-1-Data.db");
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-1-Index.db");
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-2-Data.db");
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-3-Data.db");

        expectedFiles.clear();
        for (String filePath : files)
        {
            File file = new File(filePath);
            genTestFile(file);
            expectedFiles.add(file.getAbsolutePath());
        }
    }

    public static void genTestFile(File file)
    {
        try
        {
            File parent = file.getParentFile();
            if (!parent.exists())
                parent.mkdirs();
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
            for (long i = 0; i < (5L * 1024); i++)
                bos1.write((byte) 8);
            bos1.flush();
            bos1.close();
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
        }
    }

    public static void cleanup(File dir)
    {
        FileUtils.deleteQuietly(dir);
    }

    // Mock Nodeprobe class
    @Ignore
    public static class MockNodeProbe
    {
        @Mock
        public void $init(String host, int port) throws IOException, InterruptedException
        {
        }

        @Mock
        public void takeSnapshot(String snapshotName, String columnFamily, String... keyspaces) throws IOException
        {
            File tmp = new File("target/data/");
            if (tmp.exists())
                cleanup(tmp);
            // Setup
            Set<String> files = new HashSet<String>();
            files.add("target/data/Keyspace1/Standard1/snapshots/" + snapshotName + "/Keyspace1-Standard1-ia-5-Data.db");
            files.add("target/data/Keyspace1/Standard1/snapshots/201101081230/Keyspace1-Standard1-ia-6-Data.db");
            files.add("target/data/Keyspace1/Standard1/snapshots/" + snapshotName + "/Keyspace1-Standard1-ia-7-Data.db");

            expectedFiles.clear();
            for (String filePath : files)
            {
                File file = new File(filePath);
                genTestFile(file);
                if (filePath.indexOf("Keyspace1-Standard1-ia-6-Data.db") == -1)// skip
                    expectedFiles.add(file.getAbsolutePath());
            }
        }

        @Mock
        public void close() throws IOException
        {
        }

        @Mock
        public void clearSnapshot(String tag, String... keyspaces) throws IOException
        {
            cleanup(new File("target/data"));
        }
    }

}
