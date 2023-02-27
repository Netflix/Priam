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

import com.netflix.priam.backup.*;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.token.TokenRetriever;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.Fraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to TTL or delete the SSTable components from the backups after they are not
 * referenced in the backups for more than {@link IConfiguration#getBackupRetentionDays()}. This
 * operation is executed on CRON and is configured via {@link
 * IBackupRestoreConfig#getBackupTTLMonitorPeriodInSec()}.
 *
 * <p>To TTL the SSTable components we refer to the first manifest file on the remote file system
 * after the TTL period. Any sstable components referenced in that manifest file should not be
 * deleted. Any other sstable components (files) on remote file system before the TTL period can be
 * safely deleted. Created by aagrawal on 11/26/18.
 */
@Singleton
public class BackupTTLTask extends Task {
    private static final Logger logger = LoggerFactory.getLogger(BackupTTLTask.class);
    private IBackupRestoreConfig backupRestoreConfig;
    private IMetaProxy metaProxy;
    private IBackupFileSystem fileSystem;
    private Provider<AbstractBackupPath> abstractBackupPathProvider;
    private InstanceState instanceState;
    public static final String JOBNAME = "BackupTTLService";
    private Map<String, Boolean> filesInMeta = new HashMap<>();
    private List<Path> filesToDelete = new ArrayList<>();
    private static final Lock lock = new ReentrantLock();
    private final int BATCH_SIZE = 1000;
    private final Instant start_of_feature = DateUtil.parseInstant("201801010000");
    private final int maxWaitMillis;

    @Inject
    public BackupTTLTask(
            IConfiguration configuration,
            IBackupRestoreConfig backupRestoreConfig,
            @Named("v2") IMetaProxy metaProxy,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> abstractBackupPathProvider,
            InstanceState instanceState,
            TokenRetriever tokenRetriever)
            throws Exception {
        super(configuration);
        this.backupRestoreConfig = backupRestoreConfig;
        this.metaProxy = metaProxy;
        this.fileSystem = backupFileSystemCtx.getFileStrategy(configuration);
        this.abstractBackupPathProvider = abstractBackupPathProvider;
        this.instanceState = instanceState;
        this.maxWaitMillis =
                1_000
                        * backupRestoreConfig.getBackupTTLMonitorPeriodInSec()
                        / tokenRetriever.getRingPosition().getDenominator();
    }

    @Override
    public void execute() throws Exception {
        if (instanceState.getRestoreStatus() != null
                && instanceState.getRestoreStatus().getStatus() != null
                && instanceState.getRestoreStatus().getStatus() == Status.STARTED) {
            logger.info("Not executing the TTL Task for backups as Priam is in restore mode.");
            return;
        }

        // Do not allow more than one backupTTLService to run at the same time. This is possible
        // as this happens on CRON.
        if (!lock.tryLock()) {
            logger.warn("{} is already running! Try again later.", JOBNAME);
            throw new Exception(JOBNAME + " already running");
        }

        // Sleep a random amount but not so long that it will spill into the next token's turn.
        if (maxWaitMillis > 0) Thread.sleep(new Random().nextInt(maxWaitMillis));

        try {
            filesInMeta.clear();
            filesToDelete.clear();

            Instant dateToTtl =
                    DateUtil.getInstant().minus(config.getBackupRetentionDays(), ChronoUnit.DAYS);

            // Find the snapshot just after this date.
            List<AbstractBackupPath> metas =
                    metaProxy.findMetaFiles(
                            new DateUtil.DateRange(dateToTtl, DateUtil.getInstant()));

            if (metas.size() == 0) {
                logger.info("No meta file found and thus cannot run TTL Service");
                return;
            }

            // Get the first file after the TTL time as we get files which are sorted latest to
            // oldest.
            AbstractBackupPath metaFile = metas.get(metas.size() - 1);

            // Download the meta file to local file system.
            Path localFile = metaProxy.downloadMetaFile(metaFile);

            // Walk over the file system iterator and if not in map, it is eligible for delete.
            new MetaFileWalker().readMeta(localFile);

            logger.info("No. of component files loaded from meta file: {}", filesInMeta.size());

            // Delete the meta file downloaded locally
            FileUtils.deleteQuietly(localFile.toFile());

            // If there are no files listed in meta, do not delete. This could be a bug!!
            if (filesInMeta.isEmpty()) {
                logger.warn("Meta file was empty. This should not happen. Getting out!!");
                return;
            }

            // Delete the  old META files. We are giving start date which is so back in past to get
            // all the META files.
            // This feature did not exist in Jan 2018.
            metas =
                    metaProxy.findMetaFiles(
                            new DateUtil.DateRange(
                                    start_of_feature, dateToTtl.minus(1, ChronoUnit.HOURS)));

            if (metas != null && metas.size() != 0) {
                logger.info(
                        "Will delete(TTL) {} META files starting from: [{}]",
                        metas.size(),
                        metas.get(metas.size() - 1).getLastModified());
                for (AbstractBackupPath meta : metas) {
                    deleteFile(meta, false);
                }
            }

            Iterator<String> remoteFileLocations =
                    fileSystem.listFileSystem(getSSTPrefix(), null, null);

            /*
            We really cannot delete the files until the TTL period.
            Cassandra can flush files on file system like Index.db first and other component files later (like 30 mins). If there is a snapshot in between, then this "single" component file would not be part of the snapshot as SSTable is still not part of Cassandra's "view". Only if Cassandra could provide strong guarantees on the file system such that -

            1. All component will be flushed to disk as real SSTables only if they are part of the view. Until that happens all the files will be "tmp" files.
            2. All component flushed will have the same "last modified" file. i.e. on the first flush. Stats.db can change over time and that is OK.
            Since this is not the case, the TTL may end up deleting this file even though the file is part of the next snapshot. To avoid, this we add grace period (based on how long compaction can run) when we delete the files.
            */
            dateToTtl = dateToTtl.minus(config.getGracePeriodDaysForCompaction(), ChronoUnit.DAYS);
            logger.info(
                    "Will delete(TTL) SST_V2 files which are before this time: {}. Input: [TTL: {} days, Grace Period: {} days]",
                    dateToTtl,
                    config.getBackupRetentionDays(),
                    config.getGracePeriodDaysForCompaction());

            while (remoteFileLocations.hasNext()) {
                AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
                abstractBackupPath.parseRemote(remoteFileLocations.next());
                // If lastModifiedTime is after the dateToTTL, we should get out of this loop as
                // remote file systems always give locations which are sorted.
                if (abstractBackupPath.getLastModified().isAfter(dateToTtl)) {
                    logger.info(
                            "Breaking from TTL. Got a key which is after the TTL time: {}",
                            abstractBackupPath.getRemotePath());
                    break;
                }

                if (!filesInMeta.containsKey(abstractBackupPath.getRemotePath())) {
                    deleteFile(abstractBackupPath, false);
                } else {
                    if (logger.isDebugEnabled())
                        logger.debug(
                                "Not deleting this key as it is referenced in backups: {}",
                                abstractBackupPath.getRemotePath());
                }
            }

            // Delete remaining files.
            deleteFile(null, true);

            logger.info("Finished processing files for TTL service");
        } finally {
            lock.unlock();
        }
    }

    private void deleteFile(AbstractBackupPath path, boolean forceClear)
            throws BackupRestoreException {
        if (path != null) filesToDelete.add(Paths.get(path.getRemotePath()));

        if (forceClear || filesToDelete.size() >= BATCH_SIZE) {
            fileSystem.deleteRemoteFiles(filesToDelete);
            filesToDelete.clear();
        }
    }

    private String getSSTPrefix() {
        Path location = fileSystem.getPrefix();
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        return abstractBackupPath
                .remoteV2Prefix(location, AbstractBackupPath.BackupFileType.SST_V2)
                .toString();
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    /**
     * Interval between trying to TTL data on Remote file system.
     *
     * @param backupRestoreConfig {@link IBackupRestoreConfig#getBackupTTLMonitorPeriodInSec()} to
     *     get configuration details from priam. Use "-1" to disable the service.
     * @return the timer to be used for backup ttl service.
     * @throws Exception if the configuration is not set correctly or are not valid. This is to
     *     ensure we fail-fast.
     */
    public static TaskTimer getTimer(
            IBackupRestoreConfig backupRestoreConfig, Fraction ringPosition) throws Exception {
        int period = backupRestoreConfig.getBackupTTLMonitorPeriodInSec();
        Instant start = Instant.ofEpochSecond((long) (period * ringPosition.doubleValue()));
        return new SimpleTimer(JOBNAME, period, start);
    }

    private class MetaFileWalker extends MetaFileReader {
        @Override
        public void process(ColumnFamilyResult columnfamilyResult) {
            columnfamilyResult
                    .getSstables()
                    .forEach(
                            ssTableResult ->
                                    ssTableResult
                                            .getSstableComponents()
                                            .forEach(
                                                    fileUploadResult ->
                                                            filesInMeta.put(
                                                                    fileUploadResult
                                                                            .getBackupPath(),
                                                                    null)));
        }
    }
}
