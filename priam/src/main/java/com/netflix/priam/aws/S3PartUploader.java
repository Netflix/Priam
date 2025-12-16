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

import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import com.netflix.priam.utils.SystemUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class S3PartUploader extends BoundedExponentialRetryCallable<Void> {
    private final S3Client client;
    private final DataPart dataPart;
    private final List<CompletedPart> partETags;
    private AtomicInteger partsUploaded = null; // num of data parts successfully uploaded

    private static final Logger logger = LoggerFactory.getLogger(S3PartUploader.class);
    private static final int MAX_RETRIES = 5;
    private static final int DEFAULT_MIN_SLEEP_MS = 200;

    public S3PartUploader(S3Client client, DataPart dp, List<CompletedPart> partETags) {
        super(DEFAULT_MIN_SLEEP_MS, BoundedExponentialRetryCallable.MAX_SLEEP, MAX_RETRIES);
        this.client = client;
        this.dataPart = dp;
        this.partETags = partETags;
    }

    public S3PartUploader(
            S3Client client, DataPart dp, List<CompletedPart> partETags, AtomicInteger partsUploaded) {
        super(DEFAULT_MIN_SLEEP_MS, BoundedExponentialRetryCallable.MAX_SLEEP, MAX_RETRIES);
        this.client = client;
        this.dataPart = dp;
        this.partETags = partETags;
        this.partsUploaded = partsUploaded;
    }

    private Void uploadPart() throws SdkException, BackupRestoreException {
        System.out.println("@@@ creating upload part request");
        UploadPartRequest req = UploadPartRequest.builder()
                .bucket(dataPart.getBucketName())
                .key(dataPart.getS3key())
                .uploadId(dataPart.getUploadID())
                .partNumber(dataPart.getPartNo())
                .contentLength((long) dataPart.getPartData().length)
                .contentMD5(SystemUtils.toBase64(dataPart.getMd5()))
                .build();

        System.out.println("@@@ uploading part");
        UploadPartResponse res = client.uploadPart(req, RequestBody.fromBytes(dataPart.getPartData()));

        System.out.println("@@@ comparing md5: " + SystemUtils.toHex(dataPart.getMd5()) + " to " + res.eTag());
        // AWS SDK v2 returns ETags without quotes, but we need to compare the MD5 hex
        String expectedMd5 = SystemUtils.toHex(dataPart.getMd5());
        String actualETag = res.eTag();

        if (actualETag != null && actualETag.startsWith("\"") && actualETag.endsWith("\"")) {
            actualETag = actualETag.substring(1, actualETag.length() - 1);
        }

        if (actualETag != null && actualETag.contains("-")) {
            actualETag = actualETag.substring(0, actualETag.indexOf("-"));
        }

        if (!actualETag.equals(expectedMd5)) {
            System.out.println("@@@ MD5 mismatch for part " + dataPart.getPartNo() + " expected = " + expectedMd5 + " actual = " + actualETag);
            logger.error("MD5 mismatch for part {}: expected={}, actual={}",
                    dataPart.getPartNo(), expectedMd5, actualETag);
            throw new BackupRestoreException(
                    "Unable to match MD5 for part " + dataPart.getPartNo());
        }

        CompletedPart completedPart = CompletedPart.builder()
                .partNumber(dataPart.getPartNo())
                .eTag(res.eTag())
                .build();

        partETags.add(completedPart);
        if (this.partsUploaded != null) this.partsUploaded.incrementAndGet();
        return null;
    }

    public CompleteMultipartUploadResponse completeUpload() {
        System.out.println("@@@ Building completed multipart upload");
        partETags.sort(Comparator.comparingInt(CompletedPart::partNumber));
        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                .parts(partETags)
                .build();

        System.out.println("@@@ Building complete multipart upload request");
        CompleteMultipartUploadRequest compRequest = CompleteMultipartUploadRequest.builder()
                .bucket(dataPart.getBucketName())
                .key(dataPart.getS3key())
                .uploadId(dataPart.getUploadID())
                .multipartUpload(completedMultipartUpload)
                .build();

        System.out.println("@@@ completing multipart upload");
        return client.completeMultipartUpload(compRequest);
    }

    // Abort
    public void abortUpload() {
        AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                .bucket(dataPart.getBucketName())
                .key(dataPart.getS3key())
                .uploadId(dataPart.getUploadID())
                .build();
        client.abortMultipartUpload(abortRequest);
    }

    @Override
    public Void retriableCall() throws SdkException, BackupRestoreException {
        logger.debug(
                "Picked up part {} size {}", dataPart.getPartNo(), dataPart.getPartData().length);
        return uploadPart();
    }
}
