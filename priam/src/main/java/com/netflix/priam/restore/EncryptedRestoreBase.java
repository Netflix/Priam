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
package com.netflix.priam.restore;

import com.google.inject.Provider;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredentialGeneric;
import com.netflix.priam.backup.*;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.NamedThreadPoolExecutor;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import org.bouncycastle.util.io.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides common functionality applicable to all restore strategies
 */
public abstract class EncryptedRestoreBase extends AbstractRestore{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedRestoreBase.class);

    private String jobName;
    private ICredentialGeneric pgpCredential;
    private IFileCryptography fileCryptography;
    private ICompression compress;
    private final ThreadPoolExecutor ioIntensiveExecutor;
    private final ThreadPoolExecutor cpuIntensiveExecutor;
    private AtomicInteger count = new AtomicInteger();

    protected EncryptedRestoreBase(IConfiguration config, IBackupFileSystem fs, String jobName, Sleeper sleeper,
                                   ICassandraProcess cassProcess, Provider<AbstractBackupPath> pathProvider,
                                   InstanceIdentity instanceIdentity, RestoreTokenSelector tokenSelector, ICredentialGeneric pgpCredential,
                                   IFileCryptography fileCryptography, ICompression compress, MetaData metaData, InstanceState instanceState, IPostRestoreHook postRestoreHook) {
        super(config, fs, jobName, sleeper, pathProvider, instanceIdentity, tokenSelector, cassProcess, metaData, instanceState, postRestoreHook);

        this.jobName = jobName;
        this.pgpCredential = pgpCredential;
        this.fileCryptography = fileCryptography;
        this.compress = compress;
        ioIntensiveExecutor = new NamedThreadPoolExecutor(config.getMaxBackupDownloadThreads(), jobName+".ioExecutor");
        ioIntensiveExecutor.allowCoreThreadTimeOut(true);
        cpuIntensiveExecutor = new NamedThreadPoolExecutor(config.getMaxBackupDownloadThreads(), jobName+".cpuExecutor");
        cpuIntensiveExecutor.allowCoreThreadTimeOut(true);
        logger.info("Trying to restore cassandra cluster with filesystem: {}, RestoreStrategy: {}, Encryption: ON, Compression: {}",
                fs.getClass(), jobName, compress.getClass());
    }

    private final void downloadFileAction(final AbstractBackupPath path, final File restoreLocation, final File tempFile, final File decryptedFile) throws Exception{
        try {
            logger.info("Downloading file from: {} to: {}", path.getRemotePath(), tempFile.getAbsolutePath());
            fs.download(path, new FileOutputStream(tempFile), tempFile.getAbsolutePath());
            tracker.adjustAndAdd(path);
            logger.info("Completed downloading file from: {} to: {}", path.getRemotePath(), tempFile.getAbsolutePath());

            //Object downloaded successfully from source, decrypt it.
            //If decryption fails more than retry times, it will get enqueued for download again.
            cpuIntensiveExecutor.submit(new RetryableCallable<Void>(3, 100) {
                @Override
                public Void retriableCall() throws Exception {
                    decryptFile(restoreLocation, tempFile, decryptedFile);
                    return null;
                }
            });
        } catch (Exception ex) {
            //This behavior is retryable; therefore, lets get to a clean state before each retry.
            if (tempFile.exists()) {
                tempFile.createNewFile();
            }
            throw new Exception("Exception downloading file from: " + path.getRemotePath() + " to: " + tempFile.getAbsolutePath(), ex);
        }
    }

    private final void decryptFile(final File restoreLocation, final File srcFile, final File destinationFile) throws Exception{
        final char[] passPhrase = new String(this.pgpCredential.getValue(ICredentialGeneric.KEY.PGP_PASSWORD)).toCharArray();
        try(OutputStream fOut = new BufferedOutputStream(new FileOutputStream(destinationFile));
            InputStream in    = new BufferedInputStream(new FileInputStream(srcFile.getAbsolutePath()))) {
            InputStream encryptedDataInputStream = fileCryptography.decryptStream(in, passPhrase, srcFile.getAbsolutePath());
            Streams.pipeAll(encryptedDataInputStream, fOut);
            logger.info("Completed decrypting file: {} to final file dest: {}", srcFile.getAbsolutePath(), destinationFile.getAbsolutePath());
        } catch (Exception ex) {
            //This behavior is retryable; therefore, lets get to a clean state before each retry.
            if (destinationFile.exists()) {
                destinationFile.createNewFile();
            }
            throw new Exception("Exception during decryption file:  " + destinationFile.getAbsolutePath(), ex);
        }

        try {
            //Object decrypted successfully, now uncompress it
            //If object decompression fails more than retry times, decryption is enqueued again.
            cpuIntensiveExecutor.submit(new RetryableCallable<Void>(3, 100) {
                @Override
                public Void retriableCall() throws Exception {
                    decompressFile(srcFile, destinationFile, restoreLocation);
                    return null;
                }
            });
        }catch (Exception e)
        {
            throw new Exception("Enqueuing file for decryption. Excpetion while decompressing file: " + destinationFile);
        }
    }

    private final int decompressFile(final File tempFile, final File decryptedFile, final File restoreLocation) throws Exception{
        logger.info("Start uncompressing file: {} to the FINAL destination stream", decryptedFile.getAbsolutePath());

        try(InputStream is = new BufferedInputStream(new FileInputStream(decryptedFile));
            BufferedOutputStream finalDestination = new BufferedOutputStream(new FileOutputStream(restoreLocation))) {
            compress.decompressAndClose(is, finalDestination);
        } catch (Exception ex) {
            throw new Exception("Exception uncompressing file: " + decryptedFile.getAbsolutePath() + " to the FINAL destination stream", ex);
        }

        logger.info("Completed uncompressing file: {} to the FINAL destination stream", decryptedFile.getAbsolutePath());

        //if here, everything was successful for this object, lets remove unneeded file(s)
        if (tempFile.exists())
            tempFile.delete();

        if (decryptedFile.exists()) {
            decryptedFile.delete();
        }

        return count.decrementAndGet();
    }

    @Override
    protected final void downloadFile(final AbstractBackupPath path, final File restoreLocation) throws  Exception{
        final File tempFile = new File(restoreLocation.getAbsolutePath() + ".tmp");
        final File decryptedFile = new File(tempFile.getAbsolutePath() + ".decrypted");

        count.incrementAndGet();

        try {
            ioIntensiveExecutor.submit(new RetryableCallable<Void>() {
                @Override
                public Void retriableCall() throws Exception {
                    //== download object from source bucket
                    downloadFileAction(path, restoreLocation, tempFile, decryptedFile);
                    return null;
                }
            });
        }catch (Exception e){
            throw new Exception("Exception in download of:  " + path.getFileName() + ", msg: " + e.getLocalizedMessage(), e);
        }

    }

    @Override
    protected final void waitToComplete() {
        while (count.get() != 0) {
            try {
                sleeper.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String getName() {
        return this.jobName;
    }
}