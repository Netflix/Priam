/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.services;

import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackup;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreUtil;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.backupv2.*;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraOperations;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.time.Instant;
import java.util.*;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service will run on CRON as specified by {@link
 * IBackupRestoreConfig#getSnapshotMetaServiceCronExpression()} The intent of this service is to run
 * a full snapshot on Cassandra, get the list of the SSTables on disk and then create a
 * manifest.json file which will encapsulate the list of the files i.e. capture filesystem at a
 * moment in time. This manifest.json file will ensure the true filesystem status is exposed (for
 * external entities) and will be used in future for Priam Backup Version 2 where a file is not
 * uploaded to backup file system unless SSTable has been modified. This will lead to huge reduction
 * in storage costs and provide bandwidth back to Cassandra instead of creating/uploading snapshots.
 * Note that this component will "try" to enqueue the files to upload, but no guarantee is provided.
 * If the enqueue fails for any reason, it is considered "OK" as there will be another service
 * pushing all the files in the queue for upload (think of this like a cleanup thread and will help
 * us in "resuming" any failed backup for any reason). Created by aagrawal on 6/18/18.
 */
@Singleton
public class SnapshotMetaService extends AbstractBackup {
    public static final String JOBNAME = "SnapshotMetaService";

    private static final Logger logger = LoggerFactory.getLogger(SnapshotMetaService.class);
    private static final String SNAPSHOT_PREFIX = "snap_v2_";
    private static final String CASSANDRA_MANIFEST_FILE = "manifest.json";
    private final BackupRestoreUtil backupRestoreUtil;
    private final MetaFileWriterBuilder metaFileWriter;
    private MetaFileWriterBuilder.DataStep dataStep;
    private final MetaFileManager metaFileManager;
    private final CassandraOperations cassandraOperations;
    private String snapshotName = null;

    @Inject
    SnapshotMetaService(
            IConfiguration config,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> pathFactory,
            MetaFileWriterBuilder metaFileWriter,
            MetaFileManager metaFileManager,
            CassandraOperations cassandraOperations) {
        super(config, backupFileSystemCtx, pathFactory);
        this.cassandraOperations = cassandraOperations;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getSnapshotIncludeCFList(), config.getSnapshotExcludeCFList());
        this.metaFileWriter = metaFileWriter;
        this.metaFileManager = metaFileManager;
    }

    /**
     * Interval between generating snapshot meta file using {@link
     * com.netflix.priam.services.SnapshotMetaService}.
     *
     * @param backupRestoreConfig {@link
     *     IBackupRestoreConfig#getSnapshotMetaServiceCronExpression()} to get configuration details
     *     from priam. Use "-1" to disable the service.
     * @return the timer to be used for snapshot meta service.
     * @throws Exception if the configuration is not set correctly or are not valid. This is to
     *     ensure we fail-fast.
     */
    public static TaskTimer getTimer(IBackupRestoreConfig backupRestoreConfig) throws Exception {
        CronTimer cronTimer = null;
        String cronExpression = backupRestoreConfig.getSnapshotMetaServiceCronExpression();

        if (!StringUtils.isEmpty(cronExpression) && cronExpression.equalsIgnoreCase("-1")) {
            logger.info(
                    "Skipping SnapshotMetaService as SnapshotMetaService cron is disabled via -1.");
        } else {
            if (StringUtils.isEmpty(cronExpression)
                    || !CronExpression.isValidExpression(cronExpression))
                throw new Exception(
                        "Invalid CRON expression: "
                                + cronExpression
                                + ". Please use -1, if you wish to disable SnapshotMetaService else fix the CRON expression and try again!");

            cronTimer = new CronTimer(JOBNAME, cronExpression);
            logger.info(
                    "Starting SnapshotMetaService with CRON expression {}",
                    cronTimer.getCronExpression());
        }
        return cronTimer;
    }

    String generateSnapshotName(Instant snapshotInstant) {
        return SNAPSHOT_PREFIX + DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, snapshotInstant);
    }

    @Override
    public void execute() throws Exception {
        if (!CassandraMonitor.hasCassadraStarted()) {
            logger.debug("Cassandra has not started, hence SnapshotMetaService will not run");
            return;
        }

        try {
            Instant snapshotInstant = DateUtil.getInstant();
            snapshotName = generateSnapshotName(snapshotInstant);
            logger.info("Initializing SnapshotMetaService for taking a snapshot {}", snapshotName);

            // Perform a cleanup of old snapshot meta_v2.json files, if any, as we don't want our
            // disk to be filled by them.
            // These files may be leftover
            // 1) when Priam shutdown in middle of this service and may not be full JSON
            // 2) No permission to upload to backup file system.
            metaFileManager.cleanupOldMetaFiles();

            // TODO: enqueue all the old backup folder for upload/delete, if any, as we don't want
            // our disk to be filled by them.
            // processOldSnapshotV2Folders();

            // Take a new snapshot
            cassandraOperations.takeSnapshot(snapshotName);

            // Process the snapshot and upload the meta file.
            processSnapshot(snapshotInstant).uploadMetaFile(true);

            logger.info("Finished processing snapshot meta service");
        } catch (Exception e) {
            logger.error("Error while executing SnapshotMetaService", e);
        }
    }

    MetaFileWriterBuilder.UploadStep processSnapshot(Instant snapshotInstant) throws Exception {
        dataStep = metaFileWriter.newBuilder().startMetaFileGeneration(snapshotInstant);
        initiateBackup(SNAPSHOT_FOLDER, backupRestoreUtil);
        return dataStep.endMetaFileGeneration();
    }

    private File getValidSnapshot(File snapshotDir, String snapshotName) {
        for (File fileName : snapshotDir.listFiles())
            if (fileName.exists()
                    && fileName.isDirectory()
                    && fileName.getName().matches(snapshotName)) return fileName;
        return null;
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    private ColumnfamilyResult convertToColumnFamilyResult(
            String keyspace,
            String columnFamilyName,
            Map<String, List<FileUploadResult>> filePrefixToFileMap) {
        ColumnfamilyResult columnfamilyResult = new ColumnfamilyResult(keyspace, columnFamilyName);
        filePrefixToFileMap.forEach(
                (key, value) -> {
                    ColumnfamilyResult.SSTableResult ssTableResult =
                            new ColumnfamilyResult.SSTableResult();
                    ssTableResult.setPrefix(key);
                    ssTableResult.setSstableComponents(value);
                    columnfamilyResult.addSstable(ssTableResult);
                });
        return columnfamilyResult;
    }

    @Override
    protected void processColumnFamily(
            final String keyspace, final String columnFamily, final File backupDir)
            throws Exception {
        File snapshotDir = getValidSnapshot(backupDir, snapshotName);
        // Process this snapshot folder for the given columnFamily
        if (snapshotDir == null) {
            logger.warn("{} folder does not contain {} snapshots", backupDir, snapshotName);
            return;
        }

        logger.debug("Scanning for all SSTables in: {}", snapshotDir.getAbsolutePath());

        Map<String, List<FileUploadResult>> filePrefixToFileMap = new HashMap<>();
        Collection<File> files =
                FileUtils.listFiles(snapshotDir, FileFilterUtils.fileFileFilter(), null);

        for (File file : files) {
            if (!file.exists()) continue;

            try {
                String prefix = PrefixGenerator.getSSTFileBase(file.getName());

                if (prefix == null && file.getName().equalsIgnoreCase(CASSANDRA_MANIFEST_FILE))
                    prefix = "manifest";

                if (prefix == null) {
                    logger.error(
                            "Unknown file type with no SSTFileBase found: ",
                            file.getAbsolutePath());
                    return;
                }

                FileUploadResult fileUploadResult =
                        FileUploadResult.getFileUploadResult(keyspace, columnFamily, file);
                filePrefixToFileMap.putIfAbsent(prefix, new ArrayList<>());
                filePrefixToFileMap.get(prefix).add(fileUploadResult);
            } catch (Exception e) {
                /* If you are here it means either of the issues. In that case, do not upload the meta file.
                 * @throws  UnsupportedOperationException
                 *          if an attributes of the given type are not supported
                 * @throws  IOException
                 *          if an I/O error occurs
                 * @throws  SecurityException
                 *          In the case of the default provider, a security manager is
                 *          installed, its {@link SecurityManager#checkRead(String) checkRead}
                 *          method is invoked to check read access to the file. If this
                 *          method is invoked to read security sensitive attributes then the
                 *          security manager may be invoke to check for additional permissions.
                 */
                logger.error(
                        "Internal error while trying to generate FileUploadResult and/or reading FileAttributes for file: "
                                + file.getAbsolutePath(),
                        e);
                throw e;
            }
        }

        ColumnfamilyResult columnfamilyResult =
                convertToColumnFamilyResult(keyspace, columnFamily, filePrefixToFileMap);
        filePrefixToFileMap.clear(); // Release the resources.

        logger.debug(
                "Starting the processing of KS: {}, CF: {}, No.of SSTables: {}",
                columnfamilyResult.getKeyspaceName(),
                columnfamilyResult.getColumnfamilyName(),
                columnfamilyResult.getSstables().size());

        // TODO: Future - Ensure that all the files are en-queued for Upload. Use BackupCacheService
        // (BCS) to find the
        // location where files are uploaded and BackupUploadDownloadService(BUDS) to enque if they
        // are not.
        // Note that BUDS will be responsible for actually deleting the files after they are
        // processed as they really should not be deleted unless they are successfully uploaded.
        FileUtils.cleanDirectory(snapshotDir);
        FileUtils.deleteDirectory(snapshotDir);

        dataStep.addColumnfamilyResult(columnfamilyResult);
        logger.debug(
                "Finished processing KS: {}, CF: {}",
                columnfamilyResult.getKeyspaceName(),
                columnfamilyResult.getColumnfamilyName());
    }

    @Override
    protected void addToRemotePath(String remotePath) {
        // Do nothing
    }

    // For testing purposes only.
    void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }
}
