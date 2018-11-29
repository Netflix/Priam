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
import com.netflix.priam.backup.*;
import com.netflix.priam.backupv2.*;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraOperations;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
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
    private final IBackupRestoreConfig backupRestoreConfig;
    private final BackupRestoreUtil backupRestoreUtil;
    private final MetaFileWriterBuilder metaFileWriter;
    private MetaFileWriterBuilder.DataStep dataStep;
    private final MetaFileManager metaFileManager;
    private final CassandraOperations cassandraOperations;
    private String snapshotName = null;
    private static final Lock lock = new ReentrantLock();

    private enum MetaStep {
        META_GENERATION,
        UPLOAD_FILES
    }

    private MetaStep metaStep = MetaStep.META_GENERATION;

    @Inject
    SnapshotMetaService(
            IConfiguration config,
            IBackupRestoreConfig backupRestoreConfig,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> pathFactory,
            MetaFileWriterBuilder metaFileWriter,
            MetaFileManager metaFileManager,
            CassandraOperations cassandraOperations) {
        super(config, backupFileSystemCtx, pathFactory);
        this.backupRestoreConfig = backupRestoreConfig;
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
        String cronExpression = backupRestoreConfig.getSnapshotMetaServiceCronExpression();
        return CronTimer.getCronTimer(JOBNAME, cronExpression);
    }

    String generateSnapshotName(Instant snapshotInstant) {
        return SNAPSHOT_PREFIX + DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, snapshotInstant);
    }

    /**
     * Enqueue all the files for upload in the snapshot directory. This will only enqueue the files
     * and do not give guarantee as when they will be uploaded. It will only try to upload files
     * which matches backup version 2.0 naming conventions.
     */
    public void uploadFiles() {
        try {
            // enqueue all the old snapshot folder for upload/delete, if any, as we don't want
            // our disk to be filled by them.
            metaStep = MetaStep.UPLOAD_FILES;
            initiateBackup(SNAPSHOT_FOLDER, backupRestoreUtil);
            logger.info("Finished queuing the files for upload");
        } catch (Exception e) {
            logger.error("Error while trying to upload all the files", e);
            e.printStackTrace();
        } finally {
            metaStep = MetaStep.META_GENERATION;
        }
    }

    @Override
    public void execute() throws Exception {
        if (!CassandraMonitor.hasCassadraStarted()) {
            logger.debug("Cassandra has not started, hence SnapshotMetaService will not run");
            return;
        }

        // Do not allow more than one snapshotMetaService to run at the same time. This is possible
        // as this happens on CRON.
        if (!lock.tryLock()) {
            logger.warn("SnapshotMetaService is already running! Try again later.");
            throw new Exception("SnapshotMetaService already running");
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

            // Take a new snapshot
            cassandraOperations.takeSnapshot(snapshotName);

            // Process the snapshot and upload the meta file.
            processSnapshot(snapshotInstant).uploadMetaFile(true);

            logger.info("Finished processing snapshot meta service");

            // Upload all the files from snapshot
            uploadFiles();
        } catch (Exception e) {
            logger.error("Error while executing SnapshotMetaService", e);
        } finally {
            lock.unlock();
        }
    }

    MetaFileWriterBuilder.UploadStep processSnapshot(Instant snapshotInstant) throws Exception {
        dataStep = metaFileWriter.newBuilder().startMetaFileGeneration(snapshotInstant);
        initiateBackup(SNAPSHOT_FOLDER, backupRestoreUtil);
        return dataStep.endMetaFileGeneration();
    }

    private File getValidSnapshot(File snapshotDir, String snapshotName) {
        File[] snapshotDirectories = snapshotDir.listFiles();
        if (snapshotDirectories != null)
            for (File fileName : snapshotDirectories)
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

    private void uploadAllFiles(
            final String keyspace, final String columnFamily, final File backupDir)
            throws Exception {
        // Process all the snapshots with SNAPSHOT_PREFIX. This will ensure that we "resume" the
        // uploads of previous snapshot leftover as Priam restarted or any failure for any reason
        // (like we exhausted the wait time for upload)
        File[] snapshotDirectories = backupDir.listFiles();
        if (snapshotDirectories != null) {
            for (File snapshotDirectory : snapshotDirectories) {
                // Is it a valid SNAPSHOT_PREFIX
                if (!snapshotDirectory.getName().startsWith(SNAPSHOT_PREFIX)
                        || !snapshotDirectory.isDirectory()) continue;

                if (snapshotDirectory.list().length == 0
                        || !backupRestoreConfig.enableV2Backups()) {
                    FileUtils.cleanDirectory(snapshotDirectory);
                    FileUtils.deleteDirectory(snapshotDirectory);
                    continue;
                }

                // Process each snapshot of SNAPSHOT_PREFIX
                // We do not want to wait for completion and we just want to add them to queue. This
                // is to ensure that next run happens on time.
                upload(snapshotDirectory, AbstractBackupPath.BackupFileType.SST_V2, true, false);
            }
        }
    }

    @Override
    protected void processColumnFamily(
            final String keyspace, final String columnFamily, final File backupDir)
            throws Exception {
        switch (metaStep) {
            case META_GENERATION:
                generateMetaFile(keyspace, columnFamily, backupDir);
                break;
            case UPLOAD_FILES:
                uploadAllFiles(keyspace, columnFamily, backupDir);
                break;
            default:
                throw new Exception("Unknown meta file type: " + metaStep);
        }
    }

    private void generateMetaFile(
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
                            "Unknown file type with no SSTFileBase found: {}",
                            file.getAbsolutePath());
                    continue;
                }

                FileUploadResult fileUploadResult =
                        FileUploadResult.getFileUploadResult(keyspace, columnFamily, file);
                // Add isUploaded and remotePath here.
                try {
                    AbstractBackupPath abstractBackupPath = pathFactory.get();
                    abstractBackupPath.parseLocal(file, AbstractBackupPath.BackupFileType.SST_V2);
                    fileUploadResult.setBackupPath(abstractBackupPath.getRemotePath());
                    fileUploadResult.setUploaded(
                            fs.doesRemoteFileExist(Paths.get(fileUploadResult.getBackupPath())));
                } catch (Exception e) {
                    logger.error(
                            "Error while setting the remoteLocation or checking if file exists. Ignoring them as they are not fatal.",
                            e.getMessage());
                    e.printStackTrace();
                }

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
