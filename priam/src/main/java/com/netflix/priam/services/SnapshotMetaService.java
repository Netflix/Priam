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

import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.BackupRestoreUtil;
import com.netflix.priam.backupv2.ColumnfamilyResult;
import com.netflix.priam.backupv2.FileUploadResult;
import com.netflix.priam.backupv2.MetaFileWriter;
import com.netflix.priam.backupv2.PrefixGenerator;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
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
public class SnapshotMetaService extends Task {
    public static final String JOBNAME = "SnapshotMetaService";

    private static final Logger logger = LoggerFactory.getLogger(SnapshotMetaService.class);
    private BackupRestoreUtil backupRestoreUtil;
    private IOFileFilter fileNameFilter;
    private MetaFileWriter metaFileWriter;

    @Inject
    SnapshotMetaService(IConfiguration config, BackupNotificationMgr backupNotificationMgr, MetaFileWriter metaFileWriter) {
        super(config);
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
     * @throws Exception  if the configuration is not set correctly or are not valid. This is to ensure we fail-fast.
     **/
    public static TaskTimer getTimer(IBackupRestoreConfig backupRestoreConfig) throws Exception {
        CronTimer cronTimer = null;
        String cronExpression = backupRestoreConfig.getSnapshotMetaServiceCronExpression();

        if (StringUtils.isEmpty(cronExpression) || cronExpression.equalsIgnoreCase("-1")) {
            logger.info("Skipping SnapshotMetaService as SnapshotMetaService cron is not set or is disabled via -1.");
        } else {
            if (!CronExpression.isValidExpression(cronExpression))
                throw new Exception("Invalid CRON expression: " + cronExpression +
                        ". Please remove cron expression or set to -1, if you wish to disable SnapshotMetaService else fix the CRON expression and try again!");

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

    @Override
    public void execute() throws Exception {
        try {
            logger.info("Initializaing SnapshotMetaService");
            //Walk through all the files in this snapshot.
            Path metaFilePath = processDataDir(new File(config.getDataFileLocation()));

            //Upload the meta_v2.json.
            metaFileWriter.uploadMetaFile(metaFilePath, true);
            logger.info("Finished processing snapshot meta service");
        } catch (Exception e) {
            logger.error("Error while executing SnapshotMetaService", e);
        }

    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    /**
     * This will process the data directory given and find all columnfamilies data files, and generate meta.json file. Note that this will apply filters to the data directory as mentioned by {@link IConfiguration#getSnapshotCFFilter()} and {@link IConfiguration#getSnapshotKeyspaceFilters()}
     *
     * @param dataDir Location of the data directory.
     * @return local file location of the meta file produced. Returns null, if there is any issue.
     * @throws Exception
     */
    public Path processDataDir(File dataDir) throws Exception {
        if (!dataDir.exists()) {
            throw new IllegalArgumentException("The configured 'data file location' does not exist: "
                    + dataDir);
        }

        logger.debug("Scanning for all SSTables in: {}", dataDir.getAbsolutePath());

        metaFileWriter.startMetaFileGeneration();

        for (File keyspaceDir : dataDir.listFiles()) {
            if (keyspaceDir.isFile())
                continue;

            logger.debug("Entering {} keyspace..", keyspaceDir.getName());

            for (File columnFamilyDir : keyspaceDir.listFiles()) {

                String dirName = columnFamilyDir.getName();
                String columnFamilyName = dirName.split("-")[0];

                if (backupRestoreUtil.isFiltered(keyspaceDir.getName(), columnFamilyDir.getName())) {
                    continue;
                }

                Map<String, List<FileUploadResult>> filePrefixToFileMap = new HashMap<>();
                Collection<File> files = FileUtils.listFiles(columnFamilyDir, fileNameFilter, null);
                ColumnfamilyResult columnfamilyResult = new ColumnfamilyResult(keyspaceDir.getName(), columnFamilyName);

                files.stream().filter(file -> file.exists()).filter(file -> file.isFile()).forEach(file -> {
                    try {
                        String prefix = PrefixGenerator.getSSTFileBase(file.getName());
                        FileUploadResult fileUploadResult = FileUploadResult.getFileUploadResult(keyspaceDir.getName(), columnFamilyName, file);
                        filePrefixToFileMap.putIfAbsent(prefix, new ArrayList<>());
                        filePrefixToFileMap.get(prefix).add(fileUploadResult);
                    } catch (Exception e) {
                        logger.error("Internal error while trying to generate FileUploadResult and/or reading FileAttributes for file: " + file.getAbsolutePath(), e);
                    }
                });

                filePrefixToFileMap.entrySet().forEach(sstableEntry -> {
                    ColumnfamilyResult.SSTableResult ssTableResult = new ColumnfamilyResult.SSTableResult();
                    ssTableResult.setPrefix(sstableEntry.getKey());
                    ssTableResult.setSstableComponents(sstableEntry.getValue());
                    columnfamilyResult.addSstable(ssTableResult);
                });
                filePrefixToFileMap.clear(); //Release the resources.
                processColumnFamily(columnfamilyResult);

            } //End of columnfamily
        } //End of keyspaces

        return metaFileWriter.endMetaFileGeneration();

    }

    //Process individual column family
    private void processColumnFamily(ColumnfamilyResult columnfamilyResult) throws IOException {
        if (columnfamilyResult == null)
            return;

        logger.debug("Starting the processing of KS: {}, CF: {}, No.of SSTables: {}", columnfamilyResult.getKeyspaceName(), columnfamilyResult.getColumnfamilyName(), columnfamilyResult.getSstables().size());

        //TODO: Future - Ensure that all the files are en-queued for Upload. Use BackupCacheService (BCS) to find the
        //location where files are uploaded and BackupUploadDownloadService(BUDS) to enque if they are not.

        metaFileWriter.addColumnfamilyResult(columnfamilyResult);
        logger.debug("Finished processing KS: {}, CF: {}", columnfamilyResult.getKeyspaceName(), columnfamilyResult.getColumnfamilyName());

    }


}
