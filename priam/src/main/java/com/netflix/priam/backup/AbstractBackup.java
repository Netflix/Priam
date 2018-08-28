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
package com.netflix.priam.backup;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Abstract Backup class for uploading files to backup location
 */
public abstract class AbstractBackup extends Task {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBackup.class);
    public static final String INCREMENTAL_BACKUP_FOLDER = "backups";
    public static final String SNAPSHOT_FOLDER = "snapshots";

    protected final Provider<AbstractBackupPath> pathFactory;

    protected IBackupFileSystem fs;

    @Inject
    public AbstractBackup(IConfiguration config, IFileSystemContext backupFileSystemCtx,
                          Provider<AbstractBackupPath> pathFactory) {
        super(config);
        this.pathFactory = pathFactory;
        this.fs = backupFileSystemCtx.getFileStrategy(config);
    }

    /**
     * A means to override the type of backup strategy chosen via BackupFileSystemContext
     */
    protected void setFileSystem(IBackupFileSystem fs) {
        this.fs = fs;
    }

    /**
     * Upload files in the specified dir. Does not delete the file in case of
     * error.  The files are uploaded serially.
     *
     * @param parent Parent dir
     * @param type   Type of file (META, SST, SNAP etc)
     * @return List of files that are successfully uploaded as part of backup
     * @throws Exception when there is failure in uploading files.
     */
    protected List<AbstractBackupPath> upload(File parent, final BackupFileType type) throws Exception {
        final List<AbstractBackupPath> bps = Lists.newArrayList();
        for (final File file : parent.listFiles()) {
            //== decorate file with metadata
            final AbstractBackupPath bp = pathFactory.get();
            bp.parseLocal(file, type);

            try {
                logger.debug("About to upload file {} for backup", file.getCanonicalFile());

                AbstractBackupPath abp = new RetryableCallable<AbstractBackupPath>(3, RetryableCallable.DEFAULT_WAIT_TIME) {
                    public AbstractBackupPath retriableCall() throws Exception {
                        upload(bp);
                        file.delete();
                        return bp;
                    }
                }.call();

                if (abp != null)
                    bps.add(abp);

                addToRemotePath(abp.getRemotePath());
            } catch (Exception e) {
                //Throw exception to the caller. This will allow them to take appropriate decision.
                logger.error("Failed to upload local file {} within CF {}.", file.getCanonicalFile(), parent.getAbsolutePath(), e);
                throw e;
            }
        }
        return bps;
    }


    /**
     * Upload specified file (RandomAccessFile) with retries
     *
     * @param bp backup path to be uploaded.
     */
    protected void upload(final AbstractBackupPath bp) throws Exception {
        new RetryableCallable<Void>() {
            @Override
            public Void retriableCall() throws Exception {
                java.io.InputStream is = null;
                try {
                    is = bp.localReader();
                    if (is == null) {
                        throw new NullPointerException("Unable to get handle on file: " + bp.fileName);
                    }
                    fs.upload(bp, is);
                    bp.setCompressedFileSize(fs.getBytesUploaded());
                    return null;
                } catch (Exception e) {
                    logger.error("Exception uploading local file {},  releasing handle, and will retry.", bp.backupFile.getCanonicalFile());
                    if (is != null) {
                        is.close();
                    }
                    throw e;
                }

            }
        }.call();
    }

    protected final void initiateBackup(String monitoringFolder, BackupRestoreUtil backupRestoreUtil) throws Exception {

        File dataDir = new File(config.getDataFileLocation());
        if (!dataDir.exists()) {
            throw new IllegalArgumentException("The configured 'data file location' does not exist: "
                    + config.getDataFileLocation());
        }
        logger.debug("Scanning for backup in: {}", dataDir.getAbsolutePath());
        for (File keyspaceDir : dataDir.listFiles()) {
            if (keyspaceDir.isFile())
                continue;

            logger.debug("Entering {} keyspace..", keyspaceDir.getName());

            for (File columnFamilyDir : keyspaceDir.listFiles()) {
                File backupDir = new File(columnFamilyDir, monitoringFolder);

                if (!isValidBackupDir(keyspaceDir, backupDir)) {
                    continue;
                }

                String columnFamilyName = columnFamilyDir.getName().split("-")[0];

                if (backupRestoreUtil.isFiltered(keyspaceDir.getName(), columnFamilyDir.getName())) {
                    //Clean the backup/snapshot directory else files will keep getting accumulated.
                    SystemUtils.cleanupDir(backupDir.getAbsolutePath(), null);
                    continue;
                }

                processColumnFamily(keyspaceDir.getName(), columnFamilyName, backupDir);
            } //end processing all CFs for keyspace
        } //end processing keyspaces under the C* data dir

    }

    /**
     * Process the columnfamily in a given snapshot/backup directory.
     * @param keyspace Name of the keyspace
     * @param columnFamily Name of the columnfamily
     * @param backupDir Location of the backup/snapshot directory in that columnfamily.
     * @throws Exception throws exception if there is any error in process the directory.
     */
    protected abstract void processColumnFamily(String keyspace, String columnFamily, File backupDir) throws Exception;

    /**
     * Filters unwanted keyspaces
     */
    public boolean isValidBackupDir(File keyspaceDir, File backupDir) {
        if (!backupDir.isDirectory() && !backupDir.exists())
            return false;
        String keyspaceName = keyspaceDir.getName();
        if (BackupRestoreUtil.FILTER_KEYSPACE.contains(keyspaceName)) {
            logger.debug("{} is not consider a valid keyspace backup directory, will be bypass.", keyspaceName);
            return false;
        }

        return true;
    }

    /**
     * Adds Remote path to the list of Remote Paths
     */
    protected abstract void addToRemotePath(String remotePath);
}
