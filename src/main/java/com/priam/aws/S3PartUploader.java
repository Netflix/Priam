package com.priam.aws;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.priam.backup.BackupRestoreException;
import com.priam.utils.RetryableCallable;
import com.priam.utils.SystemUtils;

public class S3PartUploader extends RetryableCallable<Void>
{
    private static final Logger logger = LoggerFactory.getLogger(S3PartUploader.class);
    public static final int MAX_RETRIES = 5;
    private AmazonS3 client;
    private DataPart dataPart;
    private List<PartETag> partETags;

    public S3PartUploader(AmazonS3 client, DataPart dp, List<PartETag> partETags)
    {
        super(MAX_RETRIES, RetryableCallable.DEFAULT_WAIT_TIME);
        this.client = client;
        this.dataPart = dp;
        this.partETags = partETags;
    }

    private Void uploadPart() throws AmazonClientException, BackupRestoreException
    {
        UploadPartRequest req = new UploadPartRequest();
        req.setBucketName(dataPart.bucketName);
        req.setKey(dataPart.s3key);
        req.setUploadId(dataPart.uploadID);
        req.setPartNumber(dataPart.partNo);
        req.setPartSize(dataPart.partData.length);
        req.setMd5Digest(SystemUtils.toBase64(dataPart.md5));
        req.setInputStream(new ByteArrayInputStream(dataPart.partData));
        UploadPartResult res = client.uploadPart(req);
        PartETag partETag = res.getPartETag();
        if (!partETag.getETag().equals(SystemUtils.toHex(dataPart.md5)))
            throw new BackupRestoreException("Unable to match MD5 for part " + dataPart.partNo);
        partETags.add(partETag);
        return null;
    }

    public void completeUpload() throws BackupRestoreException
    {
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(dataPart.bucketName, dataPart.s3key, dataPart.uploadID, partETags);
        client.completeMultipartUpload(compRequest);
    }

    // Abort
    public void abortUpload()
    {
        AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(dataPart.bucketName, dataPart.s3key, dataPart.uploadID);
        client.abortMultipartUpload(abortRequest);
    }

    @Override
    public Void retriableCall() throws AmazonClientException, BackupRestoreException
    {
        logger.info("Picked up part " + dataPart.partNo + " size " + dataPart.partData.length);
        return uploadPart();
    }
}
