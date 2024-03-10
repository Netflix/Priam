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

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreUtil;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.Status;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Future;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A means to perform a restore. This class contains the following characteristics: - It is agnostic
 * to the source type of the restore, this is determined by the injected IBackupFileSystem. - This
 * class can be scheduled, i.e. it is a "Task". - When this class is executed, it uses its own
 * thread pool to execute the restores.
 */
public abstract class AbstractRestore extends Task implements IRestoreStrategy {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRestore.class);
    final IBackupFileSystem fs;
    final Sleeper sleeper;
    private final BackupRestoreUtil backupRestoreUtil;
    private final Provider<AbstractBackupPath> pathProvider;
    private final InstanceIdentity instanceIdentity;
    private final RestoreTokenSelector tokenSelector;
    private final ICassandraProcess cassProcess;
    private final InstanceState instanceState;
    private final IPostRestoreHook postRestoreHook;

    @Inject
    @Named("v2")
    IMetaProxy metaV2Proxy;

    public AbstractRestore(
            IConfiguration config,
            IBackupFileSystem fs,
            Sleeper sleeper,
            Provider<AbstractBackupPath> pathProvider,
            InstanceIdentity instanceIdentity,
            RestoreTokenSelector tokenSelector,
            ICassandraProcess cassProcess,
            InstanceState instanceState,
            IPostRestoreHook postRestoreHook) {
        super(config);
        this.fs = fs;
        this.sleeper = sleeper;
        this.pathProvider = pathProvider;
        this.instanceIdentity = instanceIdentity;
        this.tokenSelector = tokenSelector;
        this.cassProcess = cassProcess;
        this.instanceState = instanceState;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getRestoreIncludeCFList(), config.getRestoreExcludeCFList());
        this.postRestoreHook = postRestoreHook;
    }

    public static boolean isRestoreEnabled(IConfiguration conf, InstanceInfo instanceInfo) {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getRestoreSnapshot());
        boolean isBackedupRac =
                (CollectionUtils.isEmpty(conf.getBackupRacs())
                        || conf.getBackupRacs().contains(instanceInfo.getRac()));
        return (isRestoreMode && isBackedupRac);
    }

    private List<Future<Path>> download(Iterator<AbstractBackupPath> fsIterator) throws Exception {
        List<Future<Path>> futureList = new ArrayList<>();
        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (backupRestoreUtil.isFiltered(
                    temp.getKeyspace(), temp.getColumnFamily())) {
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
            futureList.add(downloadFile(temp));
        }
        return futureList;
    }

    private void waitForCompletion(List<Future<Path>> futureList) throws Exception {
        for (Future<Path> future : futureList) future.get();
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

    public void restore(DateUtil.DateRange dateRange) throws Exception {
        if (!postRestoreHook.hasValidParameters()) {
            throw new PostRestoreHookException("Invalid PostRestoreHook parameters");
        }

        Date endTime = new Date(dateRange.getEndTime().toEpochMilli());
        IMetaProxy metaProxy = metaV2Proxy;

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
                BigInteger restoreToken =
                        tokenSelector.getClosestToken(
                                new BigInteger(origToken),
                                new Date(dateRange.getStartTime().toEpochMilli()));
                instanceIdentity.getInstance().setToken(restoreToken.toString());
            }

            stopCassProcess();

            File dataDir = new File(config.getDataFileLocation());
            if (dataDir.exists() && dataDir.isDirectory()) FileUtils.cleanDirectory(dataDir);

            Optional<AbstractBackupPath> latestValidMetaFile =
                    BackupRestoreUtil.getLatestValidMetaPath(metaProxy, dateRange);

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

            List<AbstractBackupPath> allFiles =
                    BackupRestoreUtil.getMostRecentSnapshotPaths(
                            latestValidMetaFile.get(), metaProxy, pathProvider);
            if (!config.skipIncrementalRestore()) {
                allFiles.addAll(
                        BackupRestoreUtil.getIncrementalPaths(
                                latestValidMetaFile.get(), dateRange, metaProxy));
            }

            List<Future<Path>> futureList = new ArrayList<>(download(allFiles.iterator()));

            waitForCompletion(futureList);

            logger.info("Starting post restore hook");
            postRestoreHook.execute();
            logger.info("Completed executing post restore hook");

            instanceState.getRestoreStatus().setExecutionEndTime(LocalDateTime.now());
            instanceState.setRestoreStatus(Status.FINISHED);

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
     * @return Future of the job to track the progress of the job.
     * @throws Exception If there is any error in downloading file from the remote file system.
     */
    protected abstract Future<Path> downloadFile(final AbstractBackupPath path) throws Exception;
}
