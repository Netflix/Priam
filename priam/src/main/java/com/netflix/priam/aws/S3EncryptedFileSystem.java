/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
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
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import java.io.*;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of IBackupFileSystem for S3. The upload/download will work with ciphertext. */
@Singleton
public class S3EncryptedFileSystem extends S3FileSystemBase {

    private static final Logger logger = LoggerFactory.getLogger(S3EncryptedFileSystem.class);
    private final IFileCryptography encryptor;

    @Inject
    public S3EncryptedFileSystem(
            Provider<AbstractBackupPath> pathProvider,
            ICompression compress,
            final IConfiguration config,
            ICredential cred,
            @Named("filecryptoalgorithm") IFileCryptography fileCryptography,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr,
            InstanceInfo instanceInfo) {

        super(pathProvider, compress, config, backupMetrics, backupNotificationMgr);
        this.encryptor = fileCryptography;
        super.s3Client =
                AmazonS3Client.builder()
                        .withCredentials(cred.getAwsCredentialProvider())
                        .withRegion(instanceInfo.getRegion())
                        .build();
    }

    @Override
    protected void downloadFileImpl(Path remotePath, Path localPath) throws BackupRestoreException {
        try (OutputStream os = new FileOutputStream(localPath.toFile());
                RangeReadInputStream rris =
                        new RangeReadInputStream(
                                s3Client,
                                getBucket(),
                                super.getFileSize(remotePath),
                                remotePath.toString())) {
            /*
             * To handle use cases where decompression should be done outside of the download.  For example, the file have been compressed and then encrypted.
             * Hence, decompressing it here would compromise the decryption.
             */
            IOUtils.copyLarge(rris, os);
        } catch (Exception e) {
            throw new BackupRestoreException(
                    "Exception encountered downloading "
                            + remotePath
                            + " from S3 bucket "
                            + getBucket()
                            + ", Msg: "
                            + e.getMessage(),
                    e);
        }
    }

    @Override
    protected long uploadFileImpl(Path localPath, Path remotePath) throws BackupRestoreException {
        long chunkSize = getChunkSize(localPath);
        // initialize chunking request to aws
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(config.getBackupPrefix(), remotePath.toString());
        // Fetch the aws generated upload id for this chunking request
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        DataPart part =
                new DataPart(
                        config.getBackupPrefix(),
                        remotePath.toString(),
                        initResponse.getUploadId());
        // Metadata on number of parts to be uploaded
        List<PartETag> partETags = Lists.newArrayList();

        // Read chunks from src, compress it, and write to temp file
        File compressedDstFile = new File(localPath.toString() + ".compressed");
        if (logger.isDebugEnabled())
            logger.debug(
                    "Compressing {} with chunk size {}",
                    compressedDstFile.getAbsolutePath(),
                    chunkSize);

        try (InputStream in = new FileInputStream(localPath.toFile());
                BufferedOutputStream compressedBos =
                        new BufferedOutputStream(new FileOutputStream(compressedDstFile))) {
            Iterator<byte[]> compressedChunks = this.compress.compress(in, chunkSize);
            while (compressedChunks.hasNext()) {
                byte[] compressedChunk = compressedChunks.next();
                compressedBos.write(compressedChunk);
            }
        } catch (Exception e) {
            String message =
                    "Exception in compressing the input data during upload to EncryptedStore  Msg: "
                            + e.getMessage();
            logger.error(message, e);
            throw new BackupRestoreException(message);
        }

        // == Read compressed data, encrypt each chunk, upload it to aws
        try (BufferedInputStream compressedBis =
                new BufferedInputStream(new FileInputStream(compressedDstFile))) {
            Iterator<byte[]> chunks =
                    this.encryptor.encryptStream(compressedBis, remotePath.toString());

            // identifies this part position in the object we are uploading
            int partNum = 0;
            long encryptedFileSize = 0;

            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                // throttle upload to endpoint
                rateLimiter.acquire(chunk.length);

                DataPart dp =
                        new DataPart(
                                ++partNum,
                                chunk,
                                config.getBackupPrefix(),
                                remotePath.toString(),
                                initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);
                encryptedFileSize += chunk.length;
                executor.submit(partUploader);
            }

            executor.sleepTillEmpty();
            if (partNum != partETags.size()) {
                throw new BackupRestoreException(
                        "Number of parts("
                                + partNum
                                + ")  does not match the expected number of uploaded parts("
                                + partETags.size()
                                + ")");
            }

            // complete the aws chunking upload by providing to aws the ETag that uniquely
            // identifies the combined object datav
            CompleteMultipartUploadResult resultS3MultiPartUploadComplete =
                    new S3PartUploader(s3Client, part, partETags).completeUpload();
            checkSuccessfulUpload(resultS3MultiPartUploadComplete, localPath);
            return encryptedFileSize;
        } catch (Exception e) {
            new S3PartUploader(s3Client, part, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file: " + localPath, e);
        } finally {
            if (compressedDstFile.exists()) compressedDstFile.delete();
        }
    }
}
