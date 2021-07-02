/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.priam.backupv2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;
import com.netflix.priam.backup.*;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.CassandraOperations;
import com.netflix.priam.health.CassandraMonitor;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
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
public class SnapshotMetaTask extends AbstractBackup {
    public static final String JOBNAME = "SnapshotMetaService";

    private static final Logger logger = LoggerFactory.getLogger(SnapshotMetaTask.class);
    private static final String SNAPSHOT_PREFIX = "snap_v2_";
    private static final String CASSANDRA_MANIFEST_FILE = "manifest.json";
    private static final String CASSANDRA_SCHEMA_FILE = "schema.cql";
    private final BackupRestoreUtil backupRestoreUtil;
    private final MetaFileWriterBuilder metaFileWriter;
    private MetaFileWriterBuilder.DataStep dataStep;
    private final IMetaProxy metaProxy;
    private final CassandraOperations cassandraOperations;
    private String snapshotName = null;
    private static final Lock lock = new ReentrantLock();
    private final IBackupStatusMgr snapshotStatusMgr;
    private final InstanceIdentity instanceIdentity;
    private final ExecutorService threadPool;

    private enum MetaStep {
        META_GENERATION,
        UPLOAD_FILES
    }

    private MetaStep metaStep = MetaStep.META_GENERATION;

    @Inject
    SnapshotMetaTask(
            IConfiguration config,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> pathFactory,
            MetaFileWriterBuilder metaFileWriter,
            @Named("v2") IMetaProxy metaProxy,
            InstanceIdentity instanceIdentity,
            IBackupStatusMgr snapshotStatusMgr,
            CassandraOperations cassandraOperations) {
        super(config, backupFileSystemCtx, pathFactory);
        this.instanceIdentity = instanceIdentity;
        this.snapshotStatusMgr = snapshotStatusMgr;
        this.cassandraOperations = cassandraOperations;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getSnapshotIncludeCFList(), config.getSnapshotExcludeCFList());
        this.metaFileWriter = metaFileWriter;
        this.metaProxy = metaProxy;
        this.threadPool = Executors.newSingleThreadExecutor();
    }

    /**
     * Interval between generating snapshot meta file using {@link SnapshotMetaTask}.
     *
     * @param backupRestoreConfig {@link
     *     IBackupRestoreConfig#getSnapshotMetaServiceCronExpression()} to get configuration details
     *     from priam. Use "-1" to disable the service.
     * @param config configuration to get the data folder.
     * @return the timer to be used for snapshot meta service.
     * @throws Exception if the configuration is not set correctly or are not valid. This is to
     *     ensure we fail-fast.
     */
    public static TaskTimer getTimer(
            IConfiguration config, IBackupRestoreConfig backupRestoreConfig) throws Exception {
        TaskTimer timer =
                CronTimer.getCronTimer(
                        JOBNAME, backupRestoreConfig.getSnapshotMetaServiceCronExpression());
        if (timer == null) {
            cleanOldBackups(config);
        }
        return timer;
    }

    private static void cleanOldBackups(IConfiguration config) throws Exception {
        // Clean up all the backup directories, if any.
        Set<Path> backupPaths = AbstractBackup.getBackupDirectories(config, SNAPSHOT_FOLDER);
        for (Path backupDirPath : backupPaths)
            try (DirectoryStream<Path> directoryStream =
                    Files.newDirectoryStream(backupDirPath, path -> Files.isDirectory(path))) {
                for (Path backupDir : directoryStream) {
                    if (backupDir.toFile().getName().startsWith(SNAPSHOT_PREFIX)) {
                        FileUtils.deleteDirectory(backupDir.toFile());
                    }
                }
            }
    }

    public static boolean isBackupEnabled(
            IConfiguration configuration, IBackupRestoreConfig backupRestoreConfig)
            throws Exception {
        return (getTimer(configuration, backupRestoreConfig) != null);
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

        // Save start snapshot status
        Instant snapshotInstant = DateUtil.getInstant();
        String token = instanceIdentity.getInstance().getToken();
        BackupMetadata backupMetadata =
                new BackupMetadata(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        token,
                        new Date(snapshotInstant.toEpochMilli()));
        snapshotStatusMgr.start(backupMetadata);

        try {
            snapshotName = generateSnapshotName(snapshotInstant);
            logger.info("Initializing SnapshotMetaService for taking a snapshot {}", snapshotName);

            // Perform a cleanup of old snapshot meta_v2.json files, if any, as we don't want our
            // disk to be filled by them.
            // These files may be leftover
            // 1) when Priam shutdown in middle of this service and may not be full JSON
            // 2) No permission to upload to backup file system.
            metaProxy.cleanupOldMetaFiles();

            // Take a new snapshot
            cassandraOperations.takeSnapshot(snapshotName);
            backupMetadata.setCassandraSnapshotSuccess(true);

            // Process the snapshot and upload the meta file.
            MetaFileWriterBuilder.UploadStep uploadStep = processSnapshot(snapshotInstant);
            backupMetadata.setSnapshotLocation(
                    config.getBackupPrefix() + File.separator + uploadStep.getRemoteMetaFilePath());
            uploadStep.uploadMetaFile();

            logger.info("Finished processing snapshot meta service");

            // Upload all the files from snapshot
            uploadFiles();
            snapshotStatusMgr.finish(backupMetadata);
        } catch (Exception e) {
            logger.error("Error while executing SnapshotMetaService", e);
            snapshotStatusMgr.failed(backupMetadata);
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

    private void uploadAllFiles(final File backupDir) throws Exception {
        // Process all the snapshots with SNAPSHOT_PREFIX. This will ensure that we "resume" the
        // uploads of previous snapshot leftover as Priam restarted or any failure for any reason
        // (like we exhausted the wait time for upload)
        File[] snapshotDirectories = backupDir.listFiles();
        if (snapshotDirectories != null) {
            for (File snapshotDirectory : snapshotDirectories) {
                // Is it a valid SNAPSHOT_PREFIX
                if (!snapshotDirectory.getName().startsWith(SNAPSHOT_PREFIX)
                        || !snapshotDirectory.isDirectory()) continue;

                if (snapshotDirectory.list().length == 0) {
                    FileUtils.cleanDirectory(snapshotDirectory);
                    FileUtils.deleteDirectory(snapshotDirectory);
                    continue;
                }

                // Process each snapshot of SNAPSHOT_PREFIX
                // We do not want to wait for completion and we just want to add them to queue. This
                // is to ensure that next run happens on time.
                AbstractBackupPath.BackupFileType type = AbstractBackupPath.BackupFileType.SST_V2;
                uploadAndDeleteAllFiles(snapshotDirectory, type, true);

                // Next, upload secondary indexes
                type = AbstractBackupPath.BackupFileType.SECONDARY_INDEX_V2;
                ImmutableList<ListenableFuture<AbstractBackupPath>> futures;
                FileFilter siFilter = getSecondaryIndexDirectoryFilter(backupDir);
                File[] siFiles =
                        Optional.ofNullable(snapshotDirectory.listFiles(siFilter))
                                .orElse(new File[] {});
                for (File subDir : siFiles) {
                    futures = uploadAndDeleteAllFiles(subDir, type, true);
                    if (futures.isEmpty()) {
                        deleteIfEmpty(subDir);
                    }
                    Futures.whenAllComplete(futures).call(() -> deleteIfEmpty(subDir), threadPool);
                }
            }
        }
    }

    private Void deleteIfEmpty(File dir) {
        if (FileUtils.sizeOfDirectory(dir) == 0) FileUtils.deleteQuietly(dir);
        return null;
    }

    @Override
    protected void processColumnFamily(File backupDir) throws Exception {
        String keyspace = getKeyspace(backupDir);
        String columnFamily = getColumnFamily(backupDir);
        switch (metaStep) {
            case META_GENERATION:
                generateMetaFile(keyspace, columnFamily, backupDir);
                break;
            case UPLOAD_FILES:
                uploadAllFiles(backupDir);
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
        ImmutableSetMultimap.Builder<String, AbstractBackupPath> builder =
                ImmutableSetMultimap.builder();
        builder.putAll(getSSTables(snapshotDir, AbstractBackupPath.BackupFileType.SST_V2));

        // Next, add secondary indexes
        FileFilter siFilter = getSecondaryIndexDirectoryFilter(backupDir);
        File[] secondaryIndexDirectories =
                Optional.ofNullable(snapshotDir.listFiles(siFilter)).orElse(new File[] {});
        for (File directory : secondaryIndexDirectories) {
            builder.putAll(
                    getSSTables(directory, AbstractBackupPath.BackupFileType.SECONDARY_INDEX_V2));
        }

        ImmutableSetMultimap<String, AbstractBackupPath> sstables = builder.build();
        logger.debug("Processing {} sstables from {}.{}", keyspace, columnFamily, sstables.size());
        dataStep.addColumnfamilyResult(keyspace, columnFamily, sstables);
        logger.debug("Finished processing KS: {}, CF: {}", keyspace, columnFamily);
    }

    private ImmutableSetMultimap<String, AbstractBackupPath> getSSTables(
            File snapshotDir, AbstractBackupPath.BackupFileType type) throws IOException {
        ImmutableSetMultimap.Builder<String, AbstractBackupPath> ssTables =
                ImmutableSetMultimap.builder();
        getBackupPaths(snapshotDir, type)
                .forEach(bp -> getPrefix(bp.getBackupFile()).ifPresent(p -> ssTables.put(p, bp)));
        return ssTables.build();
    }

    /**
     * Gives the prefix (common name) of the sstable components. Returns an empty Optional if it is
     * not an sstable component or a manifest or schema file.
     *
     * <p>For example: mc-3-big-Data.db -- mc-3-big ks-cf-ka-7213-Index.db -- ks-cf-ka-7213
     *
     * @param file the file from which to extract a common prefix.
     * @return common prefix of the file, or empty,
     */
    private static Optional<String> getPrefix(File file) {
        String fileName = file.getName();
        String prefix = null;
        if (fileName.contains("-")) {
            prefix = fileName.substring(0, fileName.lastIndexOf("-"));
        } else if (fileName.equalsIgnoreCase(CASSANDRA_MANIFEST_FILE)) {
            prefix = "manifest";
        } else if (fileName.equalsIgnoreCase(CASSANDRA_SCHEMA_FILE)) {
            prefix = "schema";
        } else {
            logger.error("Unknown file type with no SSTFileBase found: {}", file.getAbsolutePath());
        }
        return Optional.ofNullable(prefix);
    }

    @VisibleForTesting
    void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }
}
