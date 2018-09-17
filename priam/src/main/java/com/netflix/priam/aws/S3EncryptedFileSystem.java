/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.RangeReadInputStream;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of IBackupFileSystem for S3.  The upload/download will work with ciphertext.
 */
@Singleton
public class S3EncryptedFileSystem extends S3FileSystemBase implements S3EncryptedFileSystemMBean {

    private static final Logger logger = LoggerFactory.getLogger(S3EncryptedFileSystem.class);
    private AtomicInteger uploadCount = new AtomicInteger();
    private IFileCryptography encryptor;

    @Inject
    public S3EncryptedFileSystem(Provider<AbstractBackupPath> pathProvider, ICompression compress, final IConfiguration config, ICredential cred
            , @Named("filecryptoalgorithm") IFileCryptography fileCryptography
            , BackupMetrics backupMetrics,
                                 BackupNotificationMgr backupNotificationMgr
    ) {

        super(pathProvider, compress, config, backupMetrics, backupNotificationMgr);
        this.encryptor = fileCryptography;

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = ENCRYPTED_FILE_SYSTEM_MBEAN_NAME;
        try {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        } catch (Exception e) {
            throw new RuntimeException("Unable to regiser JMX bean: " + mbeanName + " to JMX server.  Msg: " + e.getLocalizedMessage(), e);
        }

        super.s3Client = AmazonS3Client.builder().withCredentials(cred.getAwsCredentialProvider()).withRegion(config.getDC()).build();
    }

    @Override
    /*
    Note:  provides same information as getBytesUploaded() but it's meant for S3FileSystemMBean object types.
     */
    public long bytesUploaded() {
        return bytesUploaded.get();
    }


    @Override
    public long bytesDownloaded() {
        return bytesDownloaded.get();
    }

    @Override
    public void downloadFile(AbstractBackupPath path, OutputStream os) throws BackupRestoreException {
        try {

            RangeReadInputStream rris = new RangeReadInputStream(s3Client, getPrefix(config), path);

        	/*
             * To handle use cases where decompression should be done outside of the download.  For example, the file have been compressed and then encrypted.
        	 * Hence, decompressing it here would compromise the decryption.
        	 */
            try {
                IOUtils.copyLarge(rris, os);

            } catch (Exception ex) {

                throw new BackupRestoreException("Exception encountered when copying bytes from input to output during download", ex);

            } finally {
                IOUtils.closeQuietly(rris);
                IOUtils.closeQuietly(os);
            }

        } catch (Exception e) {
            throw new BackupRestoreException("Exception encountered downloading " + path.getRemotePath() + " from S3 bucket " + getPrefix(config)
                    + ", Msg: " + e.getMessage(), e);
        }
    }


    @Override
    public void uploadFile(AbstractBackupPath path, InputStream in, long chunkSize) throws BackupRestoreException {

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), path.getRemotePath()); //initialize chunking request to aws
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest); //Fetch the aws generated upload id for this chunking request
        DataPart part = new DataPart(config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
        List<PartETag> partETags = Lists.newArrayList(); //Metadata on number of parts to be uploaded


        //== Read chunks from src, compress it, and write to temp file
        String compressedFileName = path.newRestoreFile() + ".compressed";
        logger.debug("Compressing {} with chunk size {}", compressedFileName, chunkSize);
        File compressedDstFile = null;
        FileOutputStream compressedDstFileOs = null;
        BufferedOutputStream compressedBos = null;
        try {

            compressedDstFile = new File(compressedFileName);
            compressedDstFileOs = new FileOutputStream(compressedDstFile);
            compressedBos = new BufferedOutputStream(compressedDstFileOs);

        } catch (FileNotFoundException e) {
            throw new BackupRestoreException("Not able to find temporary compressed file: " + compressedFileName);
        }

        try {

            Iterator<byte[]> compressedChunks = this.compress.compress(in, chunkSize);
            while (compressedChunks.hasNext()) {
                byte[] compressedChunk = compressedChunks.next();
                compressedBos.write(compressedChunk);
            }

        } catch (IOException e) {
            String message = String.format("Exception in compressing the input data during upload to EncryptedStore  Msg: " + e.getMessage());
            logger.error(message, e);
            throw new BackupRestoreException(message);
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(compressedBos);
        }

        //== Read compressed data, encrypt each chunk, upload it to aws
        FileInputStream compressedFileIs = null;
        BufferedInputStream compressedBis = null;
        try {

            compressedFileIs = new FileInputStream(new File(compressedFileName));
            compressedBis = new BufferedInputStream(compressedFileIs);
            Iterator<byte[]> chunks = this.encryptor.encryptStream(compressedBis, path.getRemotePath());

            int partNum = 0; //identifies this part position in the object we are uploading
            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length); //throttle upload to endpoint

                DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);
                executor.submit(partUploader);

                bytesUploaded.addAndGet(chunk.length);
            }

            executor.sleepTillEmpty();
            if (partNum != partETags.size()) {
                throw new BackupRestoreException("Number of parts(" + partNum + ")  does not match the expected number of uploaded parts(" + partETags.size() + ")");
            }

            CompleteMultipartUploadResult resultS3MultiPartUploadComplete = new S3PartUploader(s3Client, part, partETags).completeUpload(); //complete the aws chunking upload by providing to aws the ETag that uniquely identifies the combined object data
            checkSuccessfulUpload(resultS3MultiPartUploadComplete, path);

        } catch (Exception e) {
            throw encounterError(path, new S3PartUploader(s3Client, part, partETags), e);
        } finally {
            IOUtils.closeQuietly(compressedBis);
            if (compressedDstFile.exists())
                compressedDstFile.delete();
        }

    }


    @Override
    public int getActivecount() {
        return executor.getActiveCount();
    }


}