/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.restore;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.*;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.*;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.inject.Named;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A means to perform a restore. This class contains the following characteristics: - It is agnostic
 * to the source type of the restore, this is determine by the injected IBackupFileSystem. - This
 * class can be scheduled, i.e. it is a "Task". - When this class is executed, it uses its own
 * thread pool to execute the restores.
 */
public abstract class AbstractRestore extends Task implements IRestoreStrategy {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRestore.class);
    private static final String JOBNAME = "AbstractRestore";
    private static final String SYSTEM_KEYSPACE = "system";
    private static BigInteger restoreToken;
    final IBackupFileSystem fs;
    final Sleeper sleeper;
    private final BackupRestoreUtil backupRestoreUtil;
    private final Provider<AbstractBackupPath> pathProvider;
    private final InstanceIdentity instanceIdentity;
    private final RestoreTokenSelector tokenSelector;
    private final ICassandraProcess cassProcess;
    private final InstanceState instanceState;
    private final MetaData metaData;
    private final IPostRestoreHook postRestoreHook;

    @Inject
    @Named("v1")
    IMetaProxy metaV1Proxy;

    @Inject
    @Named("v2")
    IMetaProxy metaV2Proxy;

    @Inject IBackupRestoreConfig backupRestoreConfig;

    public AbstractRestore(
            IConfiguration config,
            IBackupFileSystem fs,
            String name,
            Sleeper sleeper,
            Provider<AbstractBackupPath> pathProvider,
            InstanceIdentity instanceIdentity,
            RestoreTokenSelector tokenSelector,
            ICassandraProcess cassProcess,
            MetaData metaData,
            InstanceState instanceState,
            IPostRestoreHook postRestoreHook) {
        super(config);
        this.fs = fs;
        this.sleeper = sleeper;
        this.pathProvider = pathProvider;
        this.instanceIdentity = instanceIdentity;
        this.tokenSelector = tokenSelector;
        this.cassProcess = cassProcess;
        this.metaData = metaData;
        this.instanceState = instanceState;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getRestoreIncludeCFList(), config.getRestoreExcludeCFList());
        this.postRestoreHook = postRestoreHook;
    }

    public static final boolean isRestoreEnabled(IConfiguration conf, InstanceInfo instanceInfo) {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getRestoreSnapshot());
        boolean isBackedupRac =
                (CollectionUtils.isEmpty(conf.getBackupRacs())
                        || conf.getBackupRacs().contains(instanceInfo.getRac()));
        return (isRestoreMode && isBackedupRac);
    }

    public void setRestoreConfiguration(String restoreIncludeCFList, String restoreExcludeCFList) {
        backupRestoreUtil.setFilters(restoreIncludeCFList, restoreExcludeCFList);
    }

    private List<Future<Path>> download(
            Iterator<AbstractBackupPath> fsIterator, boolean waitForCompletion) throws Exception {
        List<Future<Path>> futureList = new ArrayList<>();
        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (backupRestoreUtil.isFiltered(
                    temp.getKeyspace(), temp.getColumnFamily())) { // is filtered?
                logger.info(
                        "Bypassing restoring file \"{}\" as it is part of the keyspace.columnfamily filter list.  Its keyspace:cf is: {}:{}",
                        temp.newRestoreFile(),
                        temp.getKeyspace(),
                        temp.getColumnFamily());
                continue;
            }

            File localFileHandler = temp.newRestoreFile();
            if (logger.isDebugEnabled())
                logger.debug(
                        "Created local file name: "
                                + localFileHandler.getAbsolutePath()
                                + File.pathSeparator
                                + localFileHandler.getName());
            futureList.add(downloadFile(temp, localFileHandler));
        }

        // Wait for all download to finish that were started from this method.
        if (waitForCompletion) waitForCompletion(futureList);

        return futureList;
    }

    private void waitForCompletion(List<Future<Path>> futureList) throws Exception {
        for (Future<Path> future : futureList) future.get();
    }

    private List<Future<Path>> downloadCommitLogs(
            Iterator<AbstractBackupPath> fsIterator, int lastN, boolean waitForCompletion)
            throws Exception {
        if (fsIterator == null) return null;

        BoundedList<AbstractBackupPath> bl = new BoundedList(lastN);
        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.getType() == BackupFileType.CL) {
                bl.add(temp);
            }
        }

        return download(bl.iterator(), waitForCompletion);
    }

    private void stopCassProcess() throws IOException {
        cassProcess.stop(true);
    }

    @Override
    public void execute() throws Exception {
        if (!isRestoreEnabled(config, instanceIdentity.getInstanceInfo())) return;

        logger.info("Starting restore for {}", config.getRestoreSnapshot());
        final DateUtil.DateRange dateRange = new DateUtil.DateRange(config.getRestoreSnapshot());
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                logger.info("Attempting restore");
                restore(dateRange);
                logger.info("Restore completed");

                // Wait for other server init to complete
                sleeper.sleep(30000);
                return null;
            }
        }.call();
    }

    private Optional<AbstractBackupPath> getLatestValidMetaPath(
            IMetaProxy metaProxy, DateUtil.DateRange dateRange) {
        // Get a list of manifest files.
        List<AbstractBackupPath> metas = metaProxy.findMetaFiles(dateRange);

        // Find a valid manifest file.
        for (AbstractBackupPath meta : metas) {
            BackupVerificationResult result = metaProxy.isMetaFileValid(meta);
            if (result.valid) {
                return Optional.of(meta);
            }
        }

        return Optional.empty();
    }

    public void restore(DateUtil.DateRange dateRange) throws Exception {
        // fail early if post restore hook has invalid parameters
        if (!postRestoreHook.hasValidParameters()) {
            throw new PostRestoreHookException("Invalid PostRestoreHook parameters");
        }

        Date endTime = new Date(dateRange.getEndTime().toEpochMilli());
        IMetaProxy metaProxy = metaV1Proxy;
        if (backupRestoreConfig.enableV2Restore()) metaProxy = metaV2Proxy;

        // Set the restore status.
        instanceState.getRestoreStatus().resetStatus();
        instanceState
                .getRestoreStatus()
                .setStartDateRange(
                        LocalDateTime.ofInstant(dateRange.getStartTime(), ZoneId.of("UTC")));
        instanceState.getRestoreStatus().setEndDateRange(DateUtil.convert(endTime));
        instanceState.getRestoreStatus().setExecutionStartTime(LocalDateTime.now());
        instanceState.setRestoreStatus(Status.STARTED);
        String origToken = instanceIdentity.getInstance().getToken();

        try {
            if (config.isRestoreClosestToken()) {
                restoreToken =
                        tokenSelector.getClosestToken(
                                new BigInteger(origToken),
                                new Date(dateRange.getStartTime().toEpochMilli()));
                instanceIdentity.getInstance().setToken(restoreToken.toString());
            }

            // Stop cassandra if its running
            stopCassProcess();

            // Cleanup local data
            File dataDir = new File(config.getDataFileLocation());
            if (dataDir.exists() && dataDir.isDirectory()) FileUtils.cleanDirectory(dataDir);

            // Find latest valid meta file.
            Optional<AbstractBackupPath> latestValidMetaFile =
                    getLatestValidMetaPath(metaProxy, dateRange);

            if (!latestValidMetaFile.isPresent()) {
                logger.info("No valid snapshot meta file found, Restore Failed.");
                instanceState.getRestoreStatus().setExecutionEndTime(LocalDateTime.now());
                instanceState.setRestoreStatus(Status.FAILED);
                return;
            }

            logger.info(
                    "Snapshot Meta file for restore {}", latestValidMetaFile.get().getRemotePath());
            instanceState
                    .getRestoreStatus()
                    .setSnapshotMetaFile(latestValidMetaFile.get().getRemotePath());

            // Download the meta.json file.
            Path metaFile = metaProxy.downloadMetaFile(latestValidMetaFile.get());
            // Parse meta.json file to find the files required to download from this snapshot.
            List<AbstractBackupPath> snapshots =
                    metaProxy
                            .getSSTFilesFromMeta(metaFile)
                            .stream()
                            .map(
                                    value -> {
                                        AbstractBackupPath path = pathProvider.get();
                                        path.parseRemote(value);
                                        return path;
                                    })
                            .collect(Collectors.toList());

            FileUtils.deleteQuietly(metaFile.toFile());

            // Download snapshot which is listed in the meta file.
            List<Future<Path>> futureList = new ArrayList<>();
            futureList.addAll(download(snapshots.iterator(), false));

            logger.info("Downloading incrementals");

            // Download incrementals (SST) after the snapshot meta file.
            Instant snapshotTime;
            if (backupRestoreConfig.enableV2Restore())
                snapshotTime = latestValidMetaFile.get().getLastModified();
            else snapshotTime = latestValidMetaFile.get().getTime().toInstant();

            DateUtil.DateRange incrementalDateRange =
                    new DateUtil.DateRange(snapshotTime, dateRange.getEndTime());
            Iterator<AbstractBackupPath> incrementals =
                    metaProxy.getIncrementals(incrementalDateRange);
            futureList.addAll(download(incrementals, false));

            // Downloading CommitLogs
            // Note for Backup V2.0 we do not backup commit logs, as saving them is cost-expensive.
            if (config.isBackingUpCommitLogs()) {
                logger.info(
                        "Delete all backuped commitlog files in {}",
                        config.getBackupCommitLogLocation());
                SystemUtils.cleanupDir(config.getBackupCommitLogLocation(), null);

                logger.info("Delete all commitlog files in {}", config.getCommitLogLocation());
                SystemUtils.cleanupDir(config.getCommitLogLocation(), null);
                String prefix = fs.getPrefix().toString();
                Iterator<AbstractBackupPath> commitLogPathIterator =
                        fs.list(prefix, latestValidMetaFile.get().getTime(), endTime);
                futureList.addAll(
                        downloadCommitLogs(
                                commitLogPathIterator, config.maxCommitLogsRestore(), false));
            }

            // Wait for all the futures to finish.
            waitForCompletion(futureList);

            // Given that files are restored now, kick off post restore hook
            logger.info("Starting post restore hook");
            postRestoreHook.execute();
            logger.info("Completed executing post restore hook");

            // Declare restore as finished.
            instanceState.getRestoreStatus().setExecutionEndTime(LocalDateTime.now());
            instanceState.setRestoreStatus(Status.FINISHED);

            // Start cassandra if restore is successful.
            if (!config.doesCassandraStartManually()) cassProcess.start(true);
            else
                logger.info(
                        "config.doesCassandraStartManually() is set to True, hence Cassandra needs to be started manually ...");
        } catch (Exception e) {
            instanceState.setRestoreStatus(Status.FAILED);
            instanceState.getRestoreStatus().setExecutionEndTime(LocalDateTime.now());
            logger.error("Error while trying to restore: {}", e.getMessage(), e);
            throw e;
        } finally {
            instanceIdentity.getInstance().setToken(origToken);
        }
    }

    /**
     * Download file to the location specified. After downloading the file will be
     * decrypted(optionally) and decompressed before saving to final location.
     *
     * @param path - path of object to download from source S3/GCS.
     * @param restoreLocation - path to the final location of the decompressed and/or decrypted
     *     file.
     * @return Future of the job to track the progress of the job.
     * @throws Exception If there is any error in downloading file from the remote file system.
     */
    protected abstract Future<Path> downloadFile(
            final AbstractBackupPath path, final File restoreLocation) throws Exception;

    public final class BoundedList<E> extends LinkedList<E> {

        private final int limit;

        public BoundedList(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean add(E o) {
            super.add(o);
            while (size() > limit) {
                super.remove();
            }
            return true;
        }
    }

    public final int getDownloadTasksQueued() {
        return fs.getDownloadTasksQueued();
    }
}
