package com.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.DecoderException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.priam.utils.TokenManager;

public class TestSnapshotBackupJob
{
    private static Injector injector;
    private static final Logger logger = LoggerFactory.getLogger(TokenManager.class);

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
    }

    @Test
    public void testSnapshotList() throws BackupRestoreException, IOException, NoSuchAlgorithmException, DecoderException, InterruptedException
    {

        // TestSnapshotBackupJob.createDirs("88888");
        // File basedir = new File("cass/data");
        // ArrayList<String> fileList = new ArrayList<String>();
        // // SnapshotBackup job = injector.getInstance(SnapshotBackup.class);
        // // job.populateFiles(basedir.getAbsolutePath(), "88888", fileList);
        // for (String f : fileList)
        // System.out.println(f);
        // Assert.assertEquals(2, fileList.size());
        // TestSnapshotBackupJob.deleteDirs();
    }

    @Test
    public void testSnapshotBackupJob() throws BackupRestoreException, IOException, NoSuchAlgorithmException, DecoderException, InterruptedException
    {

        // SnapshotBackup job = injector.getInstance(SnapshotBackup.class);
        // job.setSnapshotName("10023");
        // Thread t = new Thread(job);
        // t.start();
        // t.join();

    }

    public static void createDirs(String snapshotName) throws IOException, InterruptedException
    {
        // Setup
        String[] dirs = { "cass/data/ks1/snapshots/1308981769779-" + snapshotName, "cass/data/ks1/snapshots/1308981710000-" + snapshotName, "cass/data/ks2/snapshots/1308981769779-" + snapshotName };
        String[] fileList = { dirs[0] + "/f1.db", dirs[1] + "/f2.db", dirs[2] + "/f1.db" };

        for (String dir : dirs)
        {
            File dir1 = new File(dir);
            if (!dir1.exists())
                dir1.mkdirs();
            Thread.sleep(1000);
        }
        byte b = 8;
        for (String filePath : fileList)
        {
            File file = new File(filePath);
            long fiveKB = (5L * 1024);
            System.out.println(fiveKB);
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
            for (long i = 0; i < fiveKB; i++)
            {
                bos1.write(b);
            }
            bos1.flush();
            bos1.close();
        }
        logger.info("Fake snapshot done");
    }

    public static void deleteDirs() throws IOException
    {
        new File("cass").delete();
    }
}
