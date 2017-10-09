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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredentialGeneric;
import com.netflix.priam.backup.*;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Provides common functionality applicable to all restore strategies
 */
public abstract class RestoreBase extends AbstractRestore implements IRestoreStrategy{
    private static final Logger logger = LoggerFactory.getLogger(RestoreBase.class);

    private String jobName;
    private ICredentialGeneric pgpCredential;
    private IFileCryptography fileCryptography;
    private ICompression compress;
    private MetaData metaData;

    protected RestoreBase(IConfiguration config, IBackupFileSystem fs, String jobName, Sleeper sleeper,
                          ICassandraProcess cassProcess, Provider<AbstractBackupPath> pathProvider,
                          InstanceIdentity instanceIdentity, RestoreTokenSelector tokenSelector, ICredentialGeneric pgpCredential,
                          IFileCryptography fileCryptography, ICompression compress, MetaData metaData) {
        super(config, fs, jobName, sleeper, pathProvider, instanceIdentity, tokenSelector, cassProcess);

        this.jobName = jobName;
        this.pgpCredential = pgpCredential;
        this.fileCryptography = fileCryptography;
        this.compress = compress;
        this.metaData = metaData;

        logger.info("Trying to restore cassandra cluster with filesystem: " + fs.getClass() + ", RestoreStrategy: " + jobName + ", Encryption: ON, Compression: " + compress.getClass());
    }


    @Override
    public void restore(Date startTime, Date endTime) throws Exception {
        // Stop cassandra if its running.
        stopCassProcess();

        //== Cleanup local data
        SystemUtils.cleanupDir(super.config.getDataFileLocation(), super.config.getRestoreKeySpaces());

        //== Generate Json format list of all files to download. This information is derived from meta.json file.
        List<AbstractBackupPath> metas = Lists.newArrayList();
        fetchSnapshotMetaFile(getRestorePrefix(), metas, startTime, endTime);
        if (metas.size() == 0) {
            logger.error("No snapshot meta file found, Restore Failed.");
            return;
        }


        Collections.sort(metas);
        AbstractBackupPath meta = Iterators.getLast(metas.iterator());
        logger.info("Snapshot Meta file for restore " + meta.getRemotePath());

        List<AbstractBackupPath> metaFile = new ArrayList<AbstractBackupPath>();
        metaFile.add(meta);

        //download, decrypt, and uncompress the metadata file
        performDownloadType(metaFile.iterator(), BackupFileType.META);

        List<AbstractBackupPath> snapshots = metaData.toJson(meta.newRestoreFile()); //transform the decompress, decrypted meta file to json format

        // Download the Snapshots, decrypt, and then decompress
        performDownloadType(snapshots.iterator(), BackupFileType.SNAP);

        // Download incrementals (SST).
        logger.info("Downloading incrementals");
        Iterator<AbstractBackupPath> incrementals = fs.list(getRestorePrefix(), meta.getTime(), endTime);
        logger.info("Do we have incrementals to download? " + incrementals.hasNext());
        performDownloadType(incrementals, BackupFileType.SST);

        //Downloading CommitLogs
        if (config.isBackingUpCommitLogs())  //TODO: will change to isRestoringCommitLogs()
        {
            logger.info("Delete all backuped commitlog files in " + config.getBackupCommitLogLocation());
            SystemUtils.cleanupDir(config.getBackupCommitLogLocation(), null);

            logger.info("Delete all commitlog files in " + config.getCommitLogLocation());
            SystemUtils.cleanupDir(config.getCommitLogLocation(), null);

            Iterator<AbstractBackupPath> commitLogPathIterator = fs.list(getRestorePrefix(), meta.getTime(), endTime);
            download(commitLogPathIterator, BackupFileType.CL, config.maxCommitLogsRestore());
        }
    }

    /*
     * Determines the type of download (data is encrypted or not) for the objects.
     *
     * @param fsIterator - a list of objects to download.
     * @param filer - type (e.g. SNAP, SST) of objects to download.
     * @param decryptAndDecompress - true to decrypt the download object on disk and then decompress it..
     */
    protected final void performDownloadType(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter) throws Exception {
        //download & decrypt each object
        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.getType().equals(BackupFileType.SST) && tracker.contains(temp))
                continue;

            if (temp.getType() == filter) {
                File localFileHandler = temp.newRestoreFile();
                String tempFileName = localFileHandler.getAbsolutePath() + ".tmp";
                File tempFileHandler = new File(tempFileName);

                //download from source, decrypt, and lastly uncompress

                //download(temp, localFileHandler, encryptedFileHandler, this.fileCryptography, this.keyCryptography.decrypt(null).toCharArray(), this.compress);
                String pgpPassword = new String(this.pgpCredential.getValue(ICredentialGeneric.KEY.PGP_PASSWORD));
                download(temp, localFileHandler, tempFileHandler, this.fileCryptography, pgpPassword.toCharArray(), this.compress);
            }
        }

        waitToComplete(); //wait until all objects are downloaded, decrypt, and uncompress
    }


    /**
     * A version of download commit logs, based on {@link AbstractRestore#download(Iterator, BackupFileType, int)}.
     * This version does not change the original business logic, it just assumes the commit logs are encrypted and handles it.
     */
    @Override
    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter, int lastN) throws Exception {
        if (fsIterator == null)
            return;

        BoundedList bl = new BoundedList(lastN);
        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.getType() == BackupFileType.SST && tracker.contains(temp))
                continue;

            if (temp.getType() == filter) {
                bl.add(temp);
            }
        }

        performDownloadType(bl.iterator(), filter);
    }


    @Override
    public String getName() {
        return this.jobName;
    }
}