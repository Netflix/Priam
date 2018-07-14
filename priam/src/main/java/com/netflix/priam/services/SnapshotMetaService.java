/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.services;

import com.google.inject.Provider;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackup;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreUtil;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.backupv2.ColumnfamilyResult;
import com.netflix.priam.backupv2.FileUploadResult;
import com.netflix.priam.backupv2.MetaFileWriter;
import com.netflix.priam.backupv2.PrefixGenerator;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.defaultimpl.CassandraOperations;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.DateUtil;
import org.apache.cassandra.io.sstable.Component;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * This service will run on CRON as specified by {@link IBackupRestoreConfig#getSnapshotMetaServiceCronExpression()}
 * The intent of this service is to run a full snapshot on Cassandra, get the list of the SSTables on disk
 * and then create a manifest.json file which will encapsulate the list of the files.
 * This manifest.json file will ensure the true filesystem status is exposed (for external entities) and will be
 * used in future for Priam Backup Version 2 where a file is not uploaded to backup file system unless SSTable has
 * been modified. This will lead to huge reduction in storage costs and provide bandwidth back to Cassandra instead
 * of creating/uploading snapshots.
 * Created by aagrawal on 6/18/18.
 */
@Singleton
public class SnapshotMetaService extends AbstractBackup {
    public static final String JOBNAME = "SnapshotMetaService";

    private static final Logger logger = LoggerFactory.getLogger(SnapshotMetaService.class);
    private static final String SNAPSHOT_PREFIX = "snap_v2_";
    private BackupRestoreUtil backupRestoreUtil;
    private IOFileFilter fileNameFilter;
    private MetaFileWriter metaFileWriter;
    private CassandraOperations cassandraOperations;
    private String snapshotName = null;

    @Inject
    SnapshotMetaService(IConfiguration config, IFileSystemContext backupFileSystemCtx, Provider<AbstractBackupPath> pathFactory,
                        MetaFileWriter metaFileWriter, CassandraOperations cassandraOperations) {
        super(config, backupFileSystemCtx, pathFactory);
        this.cassandraOperations = cassandraOperations;
        backupRestoreUtil = new BackupRestoreUtil(config.getSnapshotKeyspaceFilters(), config.getSnapshotCFFilter());
        initializeFileFilters();
        this.metaFileWriter = metaFileWriter;
    }

    /**
     * Interval between generating snapshot meta file using {@link com.netflix.priam.services.SnapshotMetaService}.
     *
     * @param backupRestoreConfig {@link IBackupRestoreConfig#getSnapshotMetaServiceCronExpression()} to get configuration details from priam.
     * @return the timer to be used for snapshot meta service.
     * <p>
     * It uses to generate the CRON. Use "-1" to disable
     * the service.
     * @throws Exception if the configuration is not set correctly or are not valid. This is to ensure we fail-fast.
     **/
    public static TaskTimer getTimer(IBackupRestoreConfig backupRestoreConfig) throws Exception {
        CronTimer cronTimer = null;
        String cronExpression = backupRestoreConfig.getSnapshotMetaServiceCronExpression();

        if (!StringUtils.isEmpty(cronExpression) && cronExpression.equalsIgnoreCase("-1")) {
            logger.info("Skipping SnapshotMetaService as SnapshotMetaService cron is disabled via -1.");
        } else {
            if (StringUtils.isEmpty(cronExpression) || !CronExpression.isValidExpression(cronExpression))
                throw new Exception("Invalid CRON expression: " + cronExpression +
                        ". Please use -1, if you wish to disable SnapshotMetaService else fix the CRON expression and try again!");

            cronTimer = new CronTimer(JOBNAME, cronExpression);
            logger.info("Starting SnapshotMetaService with CRON expression {}", cronTimer.getCronExpression());
        }
        return cronTimer;
    }

    private void initializeFileFilters() {
        fileNameFilter = FileFilterUtils.fileFileFilter();
        for (Component.Type type : EnumSet.allOf(Component.Type.class)) {
            fileNameFilter = FileFilterUtils.or(fileNameFilter, FileFilterUtils.suffixFileFilter(type.name()));
        }
    }

    public String generateSnapshotName(){
        return SNAPSHOT_PREFIX + DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, DateUtil.getInstant());
    }

    @Override
    public void execute() throws Exception {
        if (!CassandraMonitor.isCassadraStarted()) {
            logger.debug("Cassandra is not started, hence SnapshotMetaService will not run");
            return;
        }

        try {
            logger.info("Initializaing SnapshotMetaService");
            snapshotName = generateSnapshotName();

            //Perform a cleanup of old snapshot meta_v2.json files, if any, as we don't want our disk to be filled by them.
            //These files may be leftover
            // 1) when Priam shutdown in middle of this service and may not be full JSON
            // 2) No permission to upload to backup file system.
            metaFileWriter.cleanupOldMetaFiles();

            //TODO: enque all the old backup folder for upload/delete, if any, as we don't our disk to be filled by them.
            //processOldSnapshotV2Folders();

            //Take a new snapshot
            cassandraOperations.takeSnapshot(snapshotName);

            //Process the snapshot
            Path metaFilePath = processSnapshot();

            //Upload the meta_v2.json.
            metaFileWriter.uploadMetaFile(metaFilePath, true);
            logger.info("Finished processing snapshot meta service");
        } catch (Exception e) {
            logger.error("Error while executing SnapshotMetaService", e);
        }

    }

    public Path processSnapshot() throws Exception {
        metaFileWriter.startMetaFileGeneration();
        initiateBackup(SNAPSHOT_FOLDER, backupRestoreUtil);
        return metaFileWriter.endMetaFileGeneration();
    }

    private File getValidSnapshot(File snapshotDir, String snapshotName) {
        for (File fileName : snapshotDir.listFiles())
            if (fileName.exists() && fileName.isDirectory() && fileName.getName().matches(snapshotName))
                return fileName;
        return null;
    }

    @Override
    public String getName() {
        return JOBNAME;
    }


    private ColumnfamilyResult convertToColumnFamilyResult(String keyspace, String columnFamilyName, Map<String, List<FileUploadResult>> filePrefixToFileMap) {
        ColumnfamilyResult columnfamilyResult = new ColumnfamilyResult(keyspace, columnFamilyName);
        filePrefixToFileMap.entrySet().forEach(sstableEntry -> {
            ColumnfamilyResult.SSTableResult ssTableResult = new ColumnfamilyResult.SSTableResult();
            ssTableResult.setPrefix(sstableEntry.getKey());
            ssTableResult.setSstableComponents(sstableEntry.getValue());
            columnfamilyResult.addSstable(ssTableResult);
        });
        return columnfamilyResult;
    }

    @Override
    protected void processColumnFamily(final String keyspace, final String columnFamily, final File backupDir) throws Exception {
        File snapshotDir = getValidSnapshot(backupDir, snapshotName);
        // Process this snapshot folder for the given columnFamily
        if (snapshotDir == null) {
            logger.warn("{} folder does not contain {} snapshots", backupDir, snapshotName);
            return;
        }

        logger.debug("Scanning for all SSTables in: {}", snapshotDir.getAbsolutePath());

        Map<String, List<FileUploadResult>> filePrefixToFileMap = new HashMap<>();
        Collection<File> files = FileUtils.listFiles(snapshotDir, fileNameFilter, null);

        files.stream().filter(file -> file.exists()).filter(file -> file.isFile()).forEach(file -> {
            try {
                final String prefix = PrefixGenerator.getSSTFileBase(file.getName());
                FileUploadResult fileUploadResult = FileUploadResult.getFileUploadResult(keyspace, columnFamily, file);
                filePrefixToFileMap.putIfAbsent(prefix, new ArrayList<>());
                filePrefixToFileMap.get(prefix).add(fileUploadResult);
            } catch (Exception e) {
                logger.error("Internal error while trying to generate FileUploadResult and/or reading FileAttributes for file: " + file.getAbsolutePath(), e);
            }
        });

        ColumnfamilyResult columnfamilyResult = convertToColumnFamilyResult(keyspace, columnFamily, filePrefixToFileMap);
        filePrefixToFileMap.clear(); //Release the resources.

        logger.debug("Starting the processing of KS: {}, CF: {}, No.of SSTables: {}", columnfamilyResult.getKeyspaceName(), columnfamilyResult.getColumnfamilyName(), columnfamilyResult.getSstables().size());

        //TODO: Future - Ensure that all the files are en-queued for Upload. Use BackupCacheService (BCS) to find the
        //location where files are uploaded and BackupUploadDownloadService(BUDS) to enque if they are not.
        //Note that BUDS will be responsible for actually deleting the files after they are processed as they really should not be deleted unless they are successfully uploaded.
        FileUtils.cleanDirectory(snapshotDir);
        
        metaFileWriter.addColumnfamilyResult(columnfamilyResult);
        logger.debug("Finished processing KS: {}, CF: {}", columnfamilyResult.getKeyspaceName(), columnfamilyResult.getColumnfamilyName());

    }

    @Override
    protected void addToRemotePath(String remotePath) {
        //Do nothing
    }

    //For testing purposes only.
    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }
}
