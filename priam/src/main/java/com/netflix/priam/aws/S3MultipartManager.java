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

public class S3MultipartManager {
    private final AmazonS3 client;
    private final String bucketName;
    private final String key;
    private final String uploadId;

    private static final int MAX_RETRIES = 5;
    private static final long MAX_SLEEP = 10_000L;
    private static final long DEFAULT_MIN_SLEEP_MS = 200L;

    public static S3MultipartManager create(
            AmazonS3 client, String prefix, String remotePath, ObjectMetadata metadata) {
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(prefix, remotePath).withObjectMetadata(metadata);
        return new S3MultipartManager(client, client.initiateMultipartUpload(initRequest));
    }

    private S3MultipartManager(AmazonS3 client, InitiateMultipartUploadResult context) {
        this.client = client;
        this.bucketName = context.getBucketName();
        this.key = context.getKey();
        this.uploadId = context.getUploadId();
    }

    public BoundedExponentialRetryCallable<PartETag> getUploadTask(int partNumber, byte[] data) {
        return new BoundedExponentialRetryCallable<PartETag>(
                DEFAULT_MIN_SLEEP_MS, MAX_SLEEP, MAX_RETRIES) {
            @Override
            protected PartETag retriableCall()
                    throws AmazonClientException, BackupRestoreException {
                byte[] md5 = SystemUtils.md5(data);
                UploadPartResult result =
                        client.uploadPart(
                                new UploadPartRequest()
                                        .withBucketName(bucketName)
                                        .withKey(key)
                                        .withUploadId(uploadId)
                                        .withPartNumber(partNumber)
                                        .withPartSize(data.length)
                                        .withMD5Digest(SystemUtils.toBase64(md5))
                                        .withInputStream(new ByteArrayInputStream(data)));
                PartETag partETag = result.getPartETag();
                if (!partETag.getETag().equals(SystemUtils.toHex(md5)))
                    throw new BackupRestoreException("Unable to match MD5 for part " + partNumber);
                return partETag;
            }
        };
    }

    public CompleteMultipartUploadResult completeUpload(List<PartETag> partETags) {
        return client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags));
    }

    public void abortUpload() {
        client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
    }
}
