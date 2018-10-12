/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import com.netflix.priam.utils.SystemUtils;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3PartUploader extends BoundedExponentialRetryCallable<Void> {
    private final AmazonS3 client;
    private final DataPart dataPart;
    private final List<PartETag> partETags;
    private AtomicInteger partsUploaded = null; // num of data parts successfully uploaded

    private static final Logger logger = LoggerFactory.getLogger(S3PartUploader.class);
    private static final int MAX_RETRIES = 5;
    private static final int DEFAULT_MIN_SLEEP_MS = 200;

    public S3PartUploader(AmazonS3 client, DataPart dp, List<PartETag> partETags) {
        super(DEFAULT_MIN_SLEEP_MS, BoundedExponentialRetryCallable.MAX_SLEEP, MAX_RETRIES);
        this.client = client;
        this.dataPart = dp;
        this.partETags = partETags;
    }

    public S3PartUploader(
            AmazonS3 client, DataPart dp, List<PartETag> partETags, AtomicInteger partsUploaded) {
        super(DEFAULT_MIN_SLEEP_MS, BoundedExponentialRetryCallable.MAX_SLEEP, MAX_RETRIES);
        this.client = client;
        this.dataPart = dp;
        this.partETags = partETags;
        this.partsUploaded = partsUploaded;
    }

    private Void uploadPart() throws AmazonClientException, BackupRestoreException {
        UploadPartRequest req = new UploadPartRequest();
        req.setBucketName(dataPart.getBucketName());
        req.setKey(dataPart.getS3key());
        req.setUploadId(dataPart.getUploadID());
        req.setPartNumber(dataPart.getPartNo());
        req.setPartSize(dataPart.getPartData().length);
        req.setMd5Digest(SystemUtils.toBase64(dataPart.getMd5()));
        req.setInputStream(new ByteArrayInputStream(dataPart.getPartData()));
        UploadPartResult res = client.uploadPart(req);
        PartETag partETag = res.getPartETag();
        if (!partETag.getETag().equals(SystemUtils.toHex(dataPart.getMd5())))
            throw new BackupRestoreException(
                    "Unable to match MD5 for part " + dataPart.getPartNo());
        partETags.add(partETag);
        if (this.partsUploaded != null) this.partsUploaded.incrementAndGet();
        return null;
    }

    public CompleteMultipartUploadResult completeUpload() throws BackupRestoreException {
        CompleteMultipartUploadRequest compRequest =
                new CompleteMultipartUploadRequest(
                        dataPart.getBucketName(),
                        dataPart.getS3key(),
                        dataPart.getUploadID(),
                        partETags);
        return client.completeMultipartUpload(compRequest);
    }

    // Abort
    public void abortUpload() {
        AbortMultipartUploadRequest abortRequest =
                new AbortMultipartUploadRequest(
                        dataPart.getBucketName(), dataPart.getS3key(), dataPart.getUploadID());
        client.abortMultipartUpload(abortRequest);
    }

    @Override
    public Void retriableCall() throws AmazonClientException, BackupRestoreException {
        logger.debug(
                "Picked up part {} size {}", dataPart.getPartNo(), dataPart.getPartData().length);
        return uploadPart();
    }
}
