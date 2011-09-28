package com.priam.backup;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xerial.snappy.SnappyInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.priam.TestModule;
import com.priam.aws.ICredential;
import com.priam.aws.S3BackupPath;
import com.priam.aws.S3FileSystem;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.conf.IConfiguration;
import com.priam.identity.InstanceIdentity;

/**
 * This is test case for verifying upload. NOTE: Not a unit test. Does not mock
 * S3 Requires AWS access
 * 
 * @author psadhu
 * 
 */
public class TestUploader
{

    private static IConfiguration conf;
    private static ICredential cred;
    private static String SMALL_FILE_PATH = "cass/data/ks1/snapshots/201108082320/f1.db";
    private static String LARGE_FILE_PATH = "cass/data/ks2/snapshots/201108082320/f2.db";
    private static Injector inject;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        inject = Guice.createInjector(new TestModule());
        cred = inject.getInstance(ICredential.class);
        conf = inject.getInstance(IConfiguration.class);
        // Setup
        String[] dirs = { "cass/data/ks1/snapshots/201108082320", "cass/data/ks1/snapshots/201108082120", "cass/data/ks2/snapshots/201108082320" };
        for (String dir : dirs)
        {
            File dir1 = new File(dir);
            if (!dir1.exists())
                dir1.mkdirs();
        }
        File smallFile = new File(TestUploader.SMALL_FILE_PATH);
        File largeFile = new File(TestUploader.LARGE_FILE_PATH);

        BufferedOutputStream bos2 = new BufferedOutputStream(new FileOutputStream(largeFile));
        long tenMB = (300L * 1024 * 1024);
        byte b = 8;
        for (int i = 0; i < tenMB; i++)
        {
            bos2.write(b);
        }
        bos2.close();

        long fiveKB = (5L * 1024);
        System.out.println(fiveKB);
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(smallFile));
        for (long i = 0; i < fiveKB; i++)
        {
            bos1.write(b);
        }
        bos1.close();
    }

    @AfterClass
    public static void cleanup()
    {

        File tmpDir = new File("cass");
        if (tmpDir.exists())
            FileUtils.deleteQuietly(tmpDir);
        AWSCredentials awscred = new BasicAWSCredentials(cred.getAccessKeyId(), cred.getSecretAccessKey());
        AmazonS3 s3Client = new AmazonS3Client(awscred);
        ListObjectsRequest listReq = new ListObjectsRequest();
        listReq.setBucketName(conf.getBackupPrefix());
        listReq.setPrefix("casstestbackup");

        ObjectListing objectListing = s3Client.listObjects(listReq);
        do
        {
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries())
            {
                s3Client.deleteObject(conf.getBackupPrefix(), objectSummary.getKey());
            }
            objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } while (objectListing.isTruncated() || objectListing.getObjectSummaries().size() > 0);

    }

    @Test
    public void testLargeFileUpload() throws Exception
    {
        // Sends 2 parts
        File smallFile = new File(LARGE_FILE_PATH);
        Assert.assertTrue(smallFile.exists());
        InstanceIdentity factory = inject.getInstance(InstanceIdentity.class);
        factory.getInstance().setPayload("1234567");// Token

        S3BackupPath file = inject.getInstance(S3BackupPath.class);
        file.parseLocal(smallFile, BackupFileType.SNAP);
        Assert.assertEquals("casstestbackup/fake-region/fake-app/1234567/201108082320/SNAP/ks2/f2.db", file.getRemotePath());

        IBackupFileSystem uploader = inject.getInstance(S3FileSystem.class);
        uploader.upload(file);

        AWSCredentials awscred = new BasicAWSCredentials(TestUploader.cred.getAccessKeyId(), TestUploader.cred.getSecretAccessKey());
        AmazonS3 s3Client = new AmazonS3Client(awscred);

        // The ETag does not reflect the right MD5 in case of multi-part
        GetObjectRequest req = new GetObjectRequest(TestUploader.conf.getBackupPrefix(), file.getRemotePath());
        s3Client.getObject(req, new File("cass/f2.db.cmp.remote"));

        TestUploader.snappyDecompress("cass/f2.db.cmp.remote", "cass/f2.db");

        byte[] md5 = TestUploader.computeMD5Hash(new FileInputStream(new File(LARGE_FILE_PATH)));
        byte[] rmd5 = TestUploader.computeMD5Hash(new FileInputStream(new File("cass/f2.db")));

        String local = TestUploader.toHex(md5);
        String remote = TestUploader.toHex(rmd5);
        Assert.assertEquals(local, remote);
    }

    @Test
    public void testSmallFileUpload() throws Exception
    {
        // Sends 1 parts
        File smallFile = new File(SMALL_FILE_PATH);
        Assert.assertTrue(smallFile.exists());
        InstanceIdentity factory = inject.getInstance(InstanceIdentity.class);
        factory.getInstance().setPayload("1234567");// Token

        S3BackupPath file = inject.getInstance(S3BackupPath.class);
        file.parseLocal(smallFile, BackupFileType.SNAP);
        Assert.assertEquals("casstestbackup/fake-region/fake-app/1234567/201108082320/SNAP/ks1/f1.db", file.getRemotePath());

        IBackupFileSystem uploader = inject.getInstance(S3FileSystem.class);
        uploader.upload(file);

        AWSCredentials awscred = new BasicAWSCredentials(TestUploader.cred.getAccessKeyId(), TestUploader.cred.getSecretAccessKey());
        AmazonS3 s3Client = new AmazonS3Client(awscred);

        // The ETag does not reflect the right MD5 in case of multi-part
        GetObjectRequest req = new GetObjectRequest(TestUploader.conf.getBackupPrefix(), file.getRemotePath());
        s3Client.getObject(req, new File("cass/f1.db.cmp.remote"));

        TestUploader.snappyDecompress("cass/f1.db.cmp.remote", "cass/f1.db");

        byte[] md5 = TestUploader.computeMD5Hash(new FileInputStream(new File(SMALL_FILE_PATH)));
        byte[] rmd5 = TestUploader.computeMD5Hash(new FileInputStream(new File("cass/f1.db")));

        String local = TestUploader.toHex(md5);
        String remote = TestUploader.toHex(rmd5);
        Assert.assertEquals(local, remote);
    }

    public static void snappyDecompress(String infile, String outfile) throws IOException
    {
        FileInputStream fin = new FileInputStream(new File(infile));
        SnappyInputStream input = new SnappyInputStream(fin);

        FileOutputStream fOut = new FileOutputStream(new File(outfile));
        BufferedOutputStream bOut = new BufferedOutputStream(fOut);

        byte[] buf = new byte[8192];
        long size = 0;
        for (int readBytes = 0; (readBytes = input.read(buf)) != -1;)
        {
            bOut.write(buf, 0, readBytes);
            size += readBytes;
            // TODO fix it
            if (size >= 10)
                bOut.flush();
        }
        bOut.flush();
        bOut.close();
        input.close();
    }

    public static byte[] computeMD5Hash(InputStream is) throws NoSuchAlgorithmException, IOException
    {
        BufferedInputStream bis = new BufferedInputStream(is);
        try
        {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[16384];
            int bytesRead = -1;
            while ((bytesRead = bis.read(buffer, 0, buffer.length)) != -1)
                messageDigest.update(buffer, 0, bytesRead);
            return messageDigest.digest();
        }
        finally
        {
            bis.close();
        }
    }

    public static String toHex(byte[] data)
    {
        StringBuffer sb = new StringBuffer(data.length * 2);
        for (int i = 0; i < data.length; i++)
        {
            String hex = Integer.toHexString(data[i]);
            if (hex.length() == 1)
            {
                sb.append("0");
            }
            else if (hex.length() == 8)
            {
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase();
    }
}
