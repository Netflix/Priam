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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.RangeReadInputStream;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of IBackupFileSystem for S3
 */
@Singleton
public class S3FileSystem extends S3FileSystemBase implements S3FileSystemMBean {
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);

    @Inject
    public S3FileSystem(@Named("awss3roleassumption") IS3Credential cred, Provider<AbstractBackupPath> pathProvider,
                        ICompression compress,
                        final IConfiguration config,
                        BackupMetrics backupMetrics,
                        BackupNotificationMgr backupNotificationMgr) {
        super(pathProvider, compress, config, backupMetrics, backupNotificationMgr);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        s3Client = AmazonS3Client.builder().withCredentials(cred.getAwsCredentialProvider()).withRegion(config.getDC()).build();
    }

    @Override
    public void downloadFile(AbstractBackupPath path, OutputStream os) throws BackupRestoreException {
        try {
            RangeReadInputStream rris = new RangeReadInputStream(s3Client, getPrefix(this.config), path);
            final long bufSize = MAX_BUFFERED_IN_STREAM_SIZE > path.getSize() ? path.getSize() : MAX_BUFFERED_IN_STREAM_SIZE;
            compress.decompressAndClose(new BufferedInputStream(rris, (int) bufSize), os);
        } catch (Exception e) {
            throw new BackupRestoreException("Exception encountered downloading " + path.getRemotePath() + " from S3 bucket " + getPrefix(config)
                    + ", Msg: " + e.getMessage(), e);
        }
    }

    private void uploadMultipart(AbstractBackupPath path, InputStream in, long chunkSize) throws BackupRestoreException {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), path.getRemotePath());
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        DataPart part = new DataPart(config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
        List<PartETag> partETags = Collections.synchronizedList(new ArrayList<PartETag>());

        try {
            Iterator<byte[]> chunks = compress.compress(in, chunkSize);
            // Upload parts.
            int partNum = 0;
            AtomicInteger partsUploaded = new AtomicInteger(0);

            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length);
                DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags, partsUploaded);
                executor.submit(partUploader);
                bytesUploaded.addAndGet(chunk.length);
            }
            executor.sleepTillEmpty();
            logger.info("All chunks uploaded for file {}, num of expected parts:{}, num of actual uploaded parts: {}", path.getFileName(), partNum, partsUploaded.get());

            if (partNum != partETags.size())
                throw new BackupRestoreException("Number of parts(" + partNum + ")  does not match the uploaded parts(" + partETags.size() + ")");

            CompleteMultipartUploadResult resultS3MultiPartUploadComplete = new S3PartUploader(s3Client, part, partETags).completeUpload();
            checkSuccessfulUpload(resultS3MultiPartUploadComplete, path);

            if (logger.isDebugEnabled()) {
                final S3ResponseMetadata responseMetadata = s3Client.getCachedResponseMetadata(initRequest);
                final String requestId = responseMetadata.getRequestId(); // "x-amz-request-id" header
                final String hostId = responseMetadata.getHostId(); // "x-amz-id-2" header
                logger.debug("S3 AWS x-amz-request-id[" + requestId + "], and x-amz-id-2[" + hostId + "]");
            }

        } catch (Exception e) {
            throw encounterError(path, new S3PartUploader(s3Client, part, partETags), e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    @Override
    public void uploadFile(AbstractBackupPath path, InputStream in, long chunkSize) throws BackupRestoreException {

        if (path.getSize() < chunkSize) {
            //Upload file without using multipart upload as it will be more efficient.
            if (logger.isDebugEnabled())
                logger.debug("Uploading file using put: {}", path.getRemotePath());

            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                Iterator<byte[]> chunkedStream = compress.compress(in, chunkSize);
                while (chunkedStream.hasNext()) {
                    byteArrayOutputStream.write(chunkedStream.next());
                }
                byte[] chunk = byteArrayOutputStream.toByteArray();
                rateLimiter.acquire(chunk.length);
                ObjectMetadata objectMetadata = new ObjectMetadata();
                objectMetadata.setContentLength(chunk.length);
                PutObjectRequest putObjectRequest = new PutObjectRequest(config.getBackupPrefix(), path.getRemotePath(), new ByteArrayInputStream(chunk), objectMetadata);
                //Retry if failed.
                PutObjectResult upload = new BoundedExponentialRetryCallable<PutObjectResult>() {
                    @Override
                    public PutObjectResult retriableCall() throws Exception {
                        return s3Client.putObject(putObjectRequest);
                    }
                }.retriableCall();

                bytesUploaded.addAndGet(chunk.length);

                if (logger.isDebugEnabled())
                    logger.debug("Successfully uploaded file with putObject: {} and etag: {}", path.getRemotePath(), upload.getETag());
            } catch (Exception e) {
                throw encounterError(path, e);
            } finally {
                IOUtils.closeQuietly(in);
            }
        } else
            uploadMultipart(path, in, chunkSize);

    }

    @Override
    public int getActivecount() {
        return executor.getActiveCount();
    }


    @Override
    /*
    Note:  provides same information as getBytesUploaded() but it's meant for S3FileSystemMBean object types.
     */
    public long bytesUploaded() {
        return super.bytesUploaded.get();
    }


    @Override
    public long bytesDownloaded() {
        return bytesDownloaded.get();
    }

}