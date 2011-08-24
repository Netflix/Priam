package com.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;

import junit.framework.Assert;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.xerial.snappy.SnappyOutputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
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

/**
 * Tests to verify download and compression
 * NOTE: Not a unit test.  Requires AWS access
 * @author Praveen Sadhu
 *
 */

public class TestDownloader
{
    private static IConfiguration conf;
    private static ICredential cred;
    private static String REMOTE_FILE_PATH1 = "casstestbackup/fake-region/fake-app/1234567/201108082320/SNAP/ks1/f1.db";
    private static String REMOTE_FILE_PATH2 = "casstestbackup/fake-region/fake-app/1234567/201108082320/SNAP/ks2/f2.db";
    private static String REMOTE_SST_FILE_PATH = "casstestbackup/fake-region/fake-app/1234567/201108082320/SST/ks1/f2.db";
    private static String REMOTE_META_FILE_PATH = "casstestbackup/fake-region/fake-app/1234567/201108082320/META/1234567.meta";
    private static Injector inject;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        inject = Guice.createInjector(new TestModule());
        cred = inject.getInstance(ICredential.class);
        conf = inject.getInstance(IConfiguration.class);

        // Setup
        String[] remoteFiles = { REMOTE_FILE_PATH1, REMOTE_FILE_PATH2, REMOTE_SST_FILE_PATH, REMOTE_META_FILE_PATH };
        File tmpDir = new File("testdata");
        if (!tmpDir.exists())
            tmpDir.mkdir();
        
        // Set some local files
        String[] localFiles = { "testdata/f1.db", "testdata/f2.db", "testdata/sstf1.db", "testdata/meta.json" };

        int k = 0;
        for (String filePath : localFiles)
        {
            File tmpFile = new File(filePath);
            byte b = 8;
            long size = (50L * 1024);
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));
            for (long i = 0; i < size; i++)
            {
                bos1.write(b);
            }
            bos1.close();

            TestDownloader.snappyCompress(filePath, filePath + ".cmp");

            AWSCredentials awscred = new BasicAWSCredentials(cred.getAccessKeyId(), cred.getSecretAccessKey());
            AmazonS3 s3Client = new AmazonS3Client(awscred);
            s3Client.putObject(conf.getBackupPrefix(), remoteFiles[k], new File(filePath + ".cmp"));
            ++k;
        }

    }

    @AfterClass
    public static void cleanup()
    {
        
        File tmpDir = new File("cass");
        if (tmpDir.exists())
            FileUtils.deleteQuietly(tmpDir);
        tmpDir = new File("testdata");
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
    public void testSnapshotDownload() throws BackupRestoreException, IOException, NoSuchAlgorithmException, DecoderException, ParseException
    {

        S3BackupPath file = inject.getInstance(S3BackupPath.class);
        file.parseRemote(REMOTE_FILE_PATH1);

        IBackupFileSystem downloader = inject.getInstance(S3FileSystem.class);
        downloader.download(file);
        File f = new File("cass/data/ks1/f1.db");
        Assert.assertTrue(f.exists());

        // Check MD5 of input and output
        byte[] md5 = TestUploader.computeMD5Hash(new FileInputStream(new File("testdata/f2.db")));
        byte[] rmd5 = TestUploader.computeMD5Hash(new FileInputStream(f));

        String local = TestUploader.toHex(md5);
        String remote = TestUploader.toHex(rmd5);
        Assert.assertEquals(local, remote);
    }

    @Test
    public void testIncrDownload() throws BackupRestoreException, IOException, NoSuchAlgorithmException, DecoderException, ParseException
    {

        S3BackupPath file = inject.getInstance(S3BackupPath.class);
        file.parseRemote(REMOTE_SST_FILE_PATH);

        IBackupFileSystem downloader = inject.getInstance(S3FileSystem.class);
        downloader.download(file);
        File f = new File("cass/data/ks1/f2.db");
        Assert.assertTrue(f.exists());

        // Check MD5 of input and output
        byte[] md5 = TestUploader.computeMD5Hash(new FileInputStream(new File("testdata/sstf1.db")));
        byte[] rmd5 = TestUploader.computeMD5Hash(new FileInputStream(f));

        String local = TestUploader.toHex(md5);
        String remote = TestUploader.toHex(rmd5);
        Assert.assertEquals(local, remote);
    }

    @Test
    public void testMetaFileDownload() throws BackupRestoreException, IOException, NoSuchAlgorithmException, DecoderException, ParseException
    {
        S3BackupPath file = inject.getInstance(S3BackupPath.class);
        file.parseRemote(REMOTE_META_FILE_PATH);

        IBackupFileSystem downloader = inject.getInstance(S3FileSystem.class);
        downloader.download(file);
        File f = new File("cass/data/1234567.meta");
        Assert.assertTrue(f.exists());

        // Check MD5 of input and output
        byte[] md5 = TestUploader.computeMD5Hash(new FileInputStream(new File("testdata/meta.json")));
        byte[] rmd5 = TestUploader.computeMD5Hash(new FileInputStream(f));

        String local = TestUploader.toHex(md5);
        String remote = TestUploader.toHex(rmd5);
        Assert.assertEquals(local, remote);
    }

    @Test
    public void testUploadAndDownload() throws BackupRestoreException, IOException, NoSuchAlgorithmException, DecoderException, ParseException
    {

        // Create a file
        File dir1 = new File("cass/data/myks/snapshots/201106301240/");
        if (!dir1.exists())
            dir1.mkdirs();
        String localFile = "cass/data/myks/snapshots/201106301240/TEST-CF.data";
        File tmpFile = new File(localFile);
        byte b = 8;
        long size = (1024L);
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));
        for (long i = 0; i < size; i++)
        {
            bos1.write(b);
        }
        bos1.close();

        // Upload
        S3BackupPath ufile = inject.getInstance(S3BackupPath.class);
        ufile.parseLocal(tmpFile, BackupFileType.SNAP);
        IBackupFileSystem uploader = inject.getInstance(S3FileSystem.class);
        uploader.upload(ufile);

        S3BackupPath dBackupfile = inject.getInstance(S3BackupPath.class);
        dBackupfile.parseRemote(ufile.getRemotePath());

        IBackupFileSystem downloader = inject.getInstance(S3FileSystem.class);
        downloader.download(dBackupfile);
        Assert.assertEquals("cass/data/myks/TEST-CF.data", dBackupfile.newRestoreFile().getPath());
        File f = dBackupfile.newRestoreFile();
        Assert.assertTrue(f.exists());

        // Check MD5 of input and output
        byte[] md5 = TestUploader.computeMD5Hash(new FileInputStream(tmpFile));
        byte[] rmd5 = TestUploader.computeMD5Hash(new FileInputStream(f));

        String local = TestUploader.toHex(md5);
        String remote = TestUploader.toHex(rmd5);
        Assert.assertEquals(local, remote);
    }

    public static void snappyCompress(String infile, String outfile) throws IOException
    {
        FileInputStream fin = new FileInputStream(new File(infile));
        FileOutputStream fOut = new FileOutputStream(new File(outfile));
        BufferedOutputStream bOut = new BufferedOutputStream(fOut);

        SnappyOutputStream output = new SnappyOutputStream(bOut);

        byte[] buf = new byte[8192];
        long size = 0;
        for (int readBytes = 0; (readBytes = fin.read(buf)) != -1;)
        {
            output.write(buf, 0, readBytes);
            size += readBytes;
        }
        output.flush();
        output.close();
        fin.close();
    }
}
