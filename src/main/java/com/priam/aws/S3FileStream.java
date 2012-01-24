package com.priam.aws;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.google.common.collect.Lists;
import com.priam.backup.AbstractBackupPath;
import com.priam.backup.BackupRestoreException;
import com.priam.backup.SnappyCompression;
import com.priam.conf.IConfiguration;

public class S3FileStream
{

    private AbstractBackupPath backupfile;
    private InputStream is;
    private IConfiguration config;
    private AmazonS3 s3Client;
    private SnappyCompression compression;
    
    public S3FileStream(AmazonS3 s3Client, IConfiguration config, SnappyCompression compression, AbstractBackupPath path, InputStream is)
    {
        this.backupfile = path;
        this.is = is;
        this.config = config;
        this.s3Client = s3Client;
        this.compression = compression;
    }

    
    public String upload(){
//        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), backupfile.getRemotePath());
//        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
//        DataPart part = new DataPart(config.getBackupPrefix(), backupfile.getRemotePath(), initResponse.getUploadId());
//        List<PartETag> partETags = Lists.newArrayList();
//        try
//        {
//            Iterator<byte[]> chunks = compression.compress(backupfile.localReader());
//            // Upload parts.
//            int partNum = 0;
//            while (chunks.hasNext())
//            {
//                byte[] chunk = chunks.next();
//                DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), backupfile.getRemotePath(), initResponse.getUploadId());
//                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);                
////                executor.submit(partUploader);
////                bytesUploaded.addAndGet(chunk.length);
//            }
////            executor.sleepTillEmpty();
//            if( partNum != partETags.size())
//                throw new BackupRestoreException("Number of parts(" + partNum + ")  does not match the uploaded parts(" + partETags.size() +")");
//            new S3PartUploader(s3Client, part, partETags).completeUpload();
//        }
//        catch (Exception e)
//        {
//            new S3PartUploader(s3Client, part, partETags).abortUpload();
//            throw new BackupRestoreException("Error uploading file " + backupfile.fileName, e);
//        }

        return "";
    }
    
}
