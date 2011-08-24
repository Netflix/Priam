package com.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.instance.identity.StorageDevice;
import com.priam.FakeCLConsumer;
import com.priam.aws.EBSConsumer;
import com.priam.backup.BRTestModule;
import com.priam.backup.TestUploader;
import com.priam.conf.IConfiguration;
import com.priam.identity.InstanceIdentity;

public class TestCLBackup
{
    private static IConfiguration conf;
    private static Injector injector;
    private static int FILECOUNT = 6;
    private static int FILESIZE = 1000;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        conf = injector.getInstance(IConfiguration.class);
        TestCLBackup.cleanup();
        genFiles(conf.getCommitLogLocation());
    }

    public static void genFiles(String dir) throws IOException, InterruptedException
    {
        File cldir = new File(dir);
        if (!cldir.exists())
            cldir.mkdirs();

        for (int i = 0; i < FILECOUNT; ++i)
        {
            File tmpFile = new File(cldir, "CommitLog-" + i + ".log");
            byte b = 8;
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));
            for (long k = 0; k < FILESIZE; k++)
            {
                bos1.write(b);
            }
            bos1.close();
            Thread.sleep(1000);
            File headerFile = new File(cldir, "CommitLog-" + i + ".log.header");
            headerFile.createNewFile();
        }
    }

    @AfterClass
    public static void cleanup() throws InterruptedException, IOException
    {
        cleanupBackup();
        File tmpFile = new File(conf.getCommitLogLocation());
        if (tmpFile.exists())
            FileUtils.deleteQuietly(tmpFile);
    }

    public static void cleanupBackup() throws InterruptedException, IOException
    {
        File tmpFile = new File(conf.getBackupCommitLogLocation());
        if (tmpFile.exists())
            FileUtils.deleteQuietly(tmpFile);
    }

    @Test
    public void testCLReaderLatestFile() throws BackupRestoreException, InterruptedException
    {
        CLBackup clbackup = TestCLBackup.injector.getInstance(CLBackup.class);
        File cFile = clbackup.getLatestFile();
        Assert.assertEquals("CommitLog-" + (FILECOUNT - 1) + ".log", cFile.getName());
    }

    @Test
    public void testCLReaderRun() throws BackupRestoreException, InterruptedException, IOException
    {
        cleanupBackup();
        InstanceIdentity identity = TestCLBackup.injector.getInstance(InstanceIdentity.class);
        identity.getInstance().setVolumes(new HashMap<String, StorageDevice>());
        FakeCLConsumer consumer = new FakeCLConsumer();
        CLBackup reader = new CLBackup(conf, consumer);

        byte b = 8;
        long size = 1024 * 20;
        for (int i = 0; i < 2; ++i)
        {
            File tmpFile = new File(conf.getCommitLogLocation(), "CommitLog-" + (FILECOUNT + i) + ".log");
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));
            for (long k = 0; k < size; k++)
            {
                bos1.write(b);
                if (k % 1024 == 0)
                {
                    reader.execute();
                    bos1.flush();
                }
            }
            reader.execute();
            bos1.close();
            Thread.sleep(1000);
        }
        reader.execute();
        Assert.assertEquals(2, consumer.getFileCount());
        Assert.assertEquals(size * 2, consumer.getByteCount());
        Assert.assertEquals(FILECOUNT * 2, consumer.getHeaderCount());
    }

    @Test
    public void testEBSCLConsumer() throws BackupRestoreException, InterruptedException, IOException, NoSuchAlgorithmException
    {
        cleanupBackup();
        InstanceIdentity identity = TestCLBackup.injector.getInstance(InstanceIdentity.class);
        identity.getInstance().setVolumes(new HashMap<String, StorageDevice>());
        EBSConsumer consumer = TestCLBackup.injector.getInstance(EBSConsumer.class);
        CLBackup reader = new CLBackup(conf, consumer);
        Thread.sleep(1000);
        byte b = 8;
        long size = 1024 * 20;
        for (int i = 0; i < 2; ++i)
        {
            File tmpFile = new File(conf.getCommitLogLocation(), "CommitLog-" + (FILECOUNT + i) + ".log");
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));
            for (long k = 0; k < size; k++)
            {
                bos1.write(b);
                if (k % 1024 == 0)
                    bos1.flush();
            }
            reader.execute();
            bos1.close();
            Thread.sleep(1000);
        }

        reader.execute();
        consumer.close();
        // Verify MD5
        for (int i = 0; i < 2; ++i)
        {
            File tmpFile = new File(conf.getCommitLogLocation(), "CommitLog-" + (FILECOUNT + i) + ".log");
            byte[] md5 = TestUploader.computeMD5Hash(new FileInputStream(tmpFile));
            String srcHex = TestUploader.toHex(md5);
            File dstFile = new File(conf.getBackupCommitLogLocation(), "CommitLog-" + (FILECOUNT + i) + ".log");
            byte[] dstmd5 = TestUploader.computeMD5Hash(new FileInputStream(dstFile));
            String dstHex = TestUploader.toHex(dstmd5);
            Assert.assertEquals(srcHex, dstHex);
        }
    }

    @Test
    public void testCLRestore() throws BackupRestoreException, InterruptedException, IOException, NoSuchAlgorithmException
    {
        cleanup();
        genFiles(conf.getBackupCommitLogLocation());
        File cldir = new File(conf.getCommitLogLocation());
        cldir.mkdirs();
        InstanceIdentity identity = TestCLBackup.injector.getInstance(InstanceIdentity.class);
        identity.getInstance().setVolumes(new HashMap<String, StorageDevice>());
        EBSConsumer consumer = TestCLBackup.injector.getInstance(EBSConsumer.class);
        consumer.restore();

        for (int i = 0; i < FILECOUNT; ++i)
        {
            File tmpFile = new File(cldir, "CommitLog-" + i + ".log");
            Assert.assertTrue(tmpFile.exists());
            Assert.assertEquals(FILESIZE, tmpFile.length());
            tmpFile = new File(cldir, "CommitLog-" + i + ".log.header");
            Assert.assertTrue(tmpFile.exists());
            Assert.assertEquals(0, tmpFile.length());
        }
    }
}
