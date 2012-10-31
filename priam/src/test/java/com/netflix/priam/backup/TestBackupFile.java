package com.netflix.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.identity.InstanceIdentity;

public class TestBackupFile
{
    private static Injector injector;

    @BeforeClass
    public static void setup() throws IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        File file = new File("cass/data/ks1/", "f1.db");
        if (!file.exists())
        {
            File dir1 = new File("cass/data/ks1/");
            if (!dir1.exists())
                dir1.mkdirs();
            byte b = 8;
            long oneKB = (1L * 1024);
            System.out.println(oneKB);
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
            for (long i = 0; i < oneKB; i++)
            {
                bos1.write(b);
            }
            bos1.flush();
            bos1.close();
        }
        InstanceIdentity factory = injector.getInstance(InstanceIdentity.class);
        factory.getInstance().setToken("1234567");//Token
    }

    @AfterClass
    public static void cleanup() throws IOException
    {
        File file = new File("cass/data/ks1/f1.db");
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testBackupFileCreation() throws ParseException
    {
        // Test snapshot file
        String snapshotfile = "cass/data/ks1/snapshots/201108082320/f1.db";
        S3BackupPath backupfile = injector.getInstance(S3BackupPath.class);
        backupfile.parseLocal(new File(snapshotfile), BackupFileType.SNAP);
        Assert.assertEquals(BackupFileType.SNAP, backupfile.type);
        Assert.assertEquals("ks1", backupfile.keyspace);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals("fake-region", backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        Assert.assertEquals("casstestbackup/fake-region/fake-app/1234567/201108082320/SNAP/ks1/f1.db", backupfile.getRemotePath());
    }

    @Test
    public void testIncBackupFileCreation() throws ParseException
    {
        // Test incremental file        
        File bfile = new File("cass/data/ks1/f1.db");
        S3BackupPath backupfile = injector.getInstance(S3BackupPath.class);
        backupfile.parseLocal(bfile, BackupFileType.SST);
        Assert.assertEquals(BackupFileType.SST, backupfile.type);
        Assert.assertEquals("ks1", backupfile.keyspace);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals("fake-region", backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        String datestr = backupfile.formatDate(new Date(bfile.lastModified()));
        Assert.assertEquals("casstestbackup/fake-region/fake-app/1234567/" + datestr + "/SST/ks1/f1.db", backupfile.getRemotePath());
    }

    @Test
    public void testMetaFileCreation() throws ParseException
    {
        // Test snapshot file
        String filestr = "cass/data/1234567.meta";
        File bfile = new File(filestr);
        S3BackupPath backupfile = injector.getInstance(S3BackupPath.class);
        backupfile.time = backupfile.parseDate("201108082320");
        backupfile.parseLocal(bfile, BackupFileType.META);
        Assert.assertEquals(BackupFileType.META, backupfile.type);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals("fake-region", backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        Assert.assertEquals("casstestbackup/fake-region/fake-app/1234567/201108082320/META/1234567.meta", backupfile.getRemotePath());
    }
}
