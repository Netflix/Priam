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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.*;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.*;

/**
 * A means to perform a restore.  This class contains the following characteristics:
 * - It is agnostic to the source type of the restore, this is determine by the injected IBackupFileSystem.
 * - This class can be scheduled, i.e. it is a "Task".
 * - When this class is executed, it uses its own thread pool to execute the restores.
 */
public abstract class AbstractRestore extends Task implements IRestoreStrategy{
    // keeps track of the last few download which was executed.
    // TODO fix the magic number of 1000 => the idea of 80% of 1000 files limit per s3 query
    protected static final FifoQueue<AbstractBackupPath> tracker = new FifoQueue<AbstractBackupPath>(800);
    private static final Logger logger = LoggerFactory.getLogger(AbstractRestore.class);
    private static final String JOBNAME = "AbstractRestore";
    private static final String SYSTEM_KEYSPACE = "system";
    public static BigInteger restoreToken;
    protected final IBackupFileSystem fs;
    protected final Sleeper sleeper;
    private BackupRestoreUtil backupRestoreUtil;
    private Provider<AbstractBackupPath> pathProvider;
    private InstanceIdentity id;
    private RestoreTokenSelector tokenSelector;
    private ICassandraProcess cassProcess;
    private InstanceState instanceState;
    private MetaData metaData;

    public AbstractRestore(IConfiguration config, IBackupFileSystem fs, String name, Sleeper sleeper,
                           Provider<AbstractBackupPath> pathProvider,
                           InstanceIdentity instanceIdentity, RestoreTokenSelector tokenSelector,
                           ICassandraProcess cassProcess, MetaData metaData, InstanceState instanceState) {
        super(config);
        this.fs = fs;
        this.sleeper = sleeper;
        this.pathProvider = pathProvider;
        this.id = instanceIdentity;
        this.tokenSelector = tokenSelector;
        this.cassProcess = cassProcess;
        this.metaData = metaData;
        this.instanceState = instanceState;
        backupRestoreUtil = new BackupRestoreUtil(config.getRestoreKeyspaceFilter(), config.getRestoreCFFilter());
    }

    public static final boolean isRestoreEnabled(IConfiguration conf) {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getRestoreSnapshot());
        boolean isBackedupRac = (CollectionUtils.isEmpty(conf.getBackupRacs()) || conf.getBackupRacs().contains(conf.getRac()));
        return (isRestoreMode && isBackedupRac);
    }

    private final void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType bkupFileType) throws Exception {
        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.getType() == BackupFileType.SST && tracker.contains(temp))
                continue;

            if (backupRestoreUtil.isFiltered(BackupRestoreUtil.DIRECTORYTYPE.KEYSPACE, temp.getKeyspace())) { //keyspace filtered?
                logger.info("Bypassing restoring file \"{}\" as its keyspace: \"{}\" is part of the filter list", temp.newRestoreFile(), temp.getKeyspace());
                continue;
            }

            if (backupRestoreUtil.isFiltered(BackupRestoreUtil.DIRECTORYTYPE.CF, temp.getKeyspace(), temp.getColumnFamily())) {
                logger.info("Bypassing restoring file \"{}\" as it is part of the keyspace.columnfamily filter list.  Its keyspace:cf is: {}:{}",
                        temp.newRestoreFile(), temp.getKeyspace(), temp.getColumnFamily());
                continue;
            }

            if (config.getRestoreKeySpaces().size() != 0 && (!config.getRestoreKeySpaces().contains(temp.getKeyspace()) || temp.getKeyspace().equals(SYSTEM_KEYSPACE))) {
                logger.info("Bypassing restoring file \"{}\" as it is system keyspace", temp.newRestoreFile());
                continue;
            }

            if (temp.getType() == bkupFileType)
            {
                File localFileHandler = temp.newRestoreFile();
                if (logger.isDebugEnabled())
                    logger.debug("Created local file name: " + localFileHandler.getAbsolutePath() + File.pathSeparator + localFileHandler.getName());
                downloadFile(temp, localFileHandler);
            }
        }

        //Wait for all download to finish that were started from this method.
        waitToComplete();
    }

    private final void downloadCommitLogs(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter, int lastN) throws Exception {
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

        download(bl.iterator(), filter);
    }


    protected final void stopCassProcess() throws IOException {
        if (config.getRestoreKeySpaces().size() == 0)
            cassProcess.stop();
    }

    protected final String getRestorePrefix() {
        String prefix = "";

        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();

        return prefix;
    }

    /*
     * Fetches meta.json used to store snapshots metadata.
     */
    private final void fetchSnapshotMetaFile(String restorePrefix, List<AbstractBackupPath> out, Date startTime, Date endTime) throws IllegalStateException {
        logger.debug("Looking for snapshot meta file within restore prefix: {}", restorePrefix);

        Iterator<AbstractBackupPath> backupfiles = fs.list(restorePrefix, startTime, endTime);
        if (!backupfiles.hasNext()) {
            throw new IllegalStateException("meta.json not found, restore prefix: " + restorePrefix);
        }

        while (backupfiles.hasNext()) {
            AbstractBackupPath path = backupfiles.next();
            if (path.getType() == BackupFileType.META)
                //Since there are now meta file for incrementals as well as snapshot, we need to find the correct one (i.e. the snapshot meta file (meta.json))
                if (path.getFileName().equalsIgnoreCase("meta.json")) {
                    out.add(path);
                }
        }
    }

    @Override
    public void execute() throws Exception {
        if (!isRestoreEnabled(config))
            return;

        logger.info("Starting restore for {}", config.getRestoreSnapshot());
        String[] restore = config.getRestoreSnapshot().split(",");
        AbstractBackupPath path = pathProvider.get();
        final Date startTime = path.parseDate(restore[0]);
        final Date endTime = path.parseDate(restore[1]);
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                logger.info("Attempting restore");
                restore(startTime, endTime);
                logger.info("Restore completed");

                // Wait for other server init to complete
                sleeper.sleep(30000);
                return null;
            }
        }.call();

    }

    public void restore(Date startTime, Date endTime) throws Exception {
        //Set the restore status.
        instanceState.getRestoreStatus().resetStatus();
        instanceState.getRestoreStatus().setStartDateRange(DateUtil.convert(startTime));
        instanceState.getRestoreStatus().setEndDateRange(DateUtil.convert(endTime));
        instanceState.getRestoreStatus().setExecutionStartTime(LocalDateTime.now());
        instanceState.setRestoreStatus(Status.STARTED);
        String origBackupIdentifier = id.getBackupIdentifier();

        try {
            if (config.isRestoreClosestToken()) {
                restoreToken = tokenSelector.getClosestToken(id.getToken(), startTime);
                id.setBackupIdentifier(restoreToken.toString());
            }

            // Stop cassandra if its running and restoring all keyspaces
            stopCassProcess();

            // Cleanup local data
            SystemUtils.cleanupDir(config.getDataFileLocation(), config.getRestoreKeySpaces());

            // Try and read the Meta file.
            List<AbstractBackupPath> metas = Lists.newArrayList();
            String prefix = getRestorePrefix();
            fetchSnapshotMetaFile(prefix, metas, startTime, endTime);

            if (metas.size() == 0) {
                logger.info("[cass_backup] No snapshot meta file found, Restore Failed.");
                instanceState.getRestoreStatus().setExecutionEndTime(LocalDateTime.now());
                instanceState.setRestoreStatus(Status.FINISHED);
                return;
            }

            Collections.sort(metas);
            AbstractBackupPath meta = Iterators.getLast(metas.iterator());
            logger.info("Snapshot Meta file for restore {}", meta.getRemotePath());
            instanceState.getRestoreStatus().setSnapshotMetaFile(meta.getRemotePath());

            //Download the meta.json file.
            ArrayList<AbstractBackupPath> metaFile = new ArrayList<>();
            metaFile.add(meta);
            download(metaFile.iterator(), BackupFileType.META);
            waitToComplete();

            //Parse meta.json file to find the files required to download from this snapshot.
            List<AbstractBackupPath> snapshots = metaData.toJson(meta.newRestoreFile());

            // Download snapshot which is listed in the meta file.
            download(snapshots.iterator(), BackupFileType.SNAP);

            logger.info("Downloading incrementals");
            // Download incrementals (SST) after the snapshot meta file.
            Iterator<AbstractBackupPath> incrementals = fs.list(prefix, meta.getTime(), endTime);
            download(incrementals, BackupFileType.SST);

            //Downloading CommitLogs
            if (config.isBackingUpCommitLogs()) {
                logger.info("Delete all backuped commitlog files in {}", config.getBackupCommitLogLocation());
                SystemUtils.cleanupDir(config.getBackupCommitLogLocation(), null);

                logger.info("Delete all commitlog files in {}", config.getCommitLogLocation());
                SystemUtils.cleanupDir(config.getCommitLogLocation(), null);

                Iterator<AbstractBackupPath> commitLogPathIterator = fs.list(prefix, meta.getTime(), endTime);
                downloadCommitLogs(commitLogPathIterator, BackupFileType.CL, config.maxCommitLogsRestore());
            }

            //Ensure all the files are downloaded before declaring restore as finished.
            waitToComplete();
            instanceState.getRestoreStatus().setExecutionEndTime(LocalDateTime.now());
            instanceState.setRestoreStatus(Status.FINISHED);

            //Start cassandra if restore is successful.
            cassProcess.start(true);
        } catch (Exception e) {
            instanceState.setRestoreStatus(Status.FAILED);
            instanceState.getRestoreStatus().setExecutionEndTime(LocalDateTime.now());
            logger.error("Error while trying to restore: {}", e.getMessage(), e);
            throw e;
        } finally {
            id.setBackupIdentifier(origBackupIdentifier);
        }
    }

    /**
     * Download file to the location specified. After downloading the file will be decrypted(optionally) and decompressed before saving to final location.
     * @param path            - path of object to download from source S3/GCS.
     * @param restoreLocation - path to the final location of the decompressed and/or decrypted file.
     */
    protected abstract void downloadFile(final AbstractBackupPath path, final File restoreLocation) throws Exception;

    /**
     * A means to wait until until all threads have completed.  It blocks calling thread
     * until all tasks are completed.
     */
    protected abstract void waitToComplete();

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
}