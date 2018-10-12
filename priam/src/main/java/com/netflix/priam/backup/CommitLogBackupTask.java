/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.IMessageObserver.BACKUP_MESSAGE_TYPE;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Provide this to be run as a Quart job
@Singleton
public class CommitLogBackupTask extends AbstractBackup {
    public static final String JOBNAME = "CommitLogBackup";

    private static final Logger logger = LoggerFactory.getLogger(CommitLogBackupTask.class);
    private final List<String> clRemotePaths = new ArrayList<>();
    private static final List<IMessageObserver> observers = new ArrayList<>();
    private final CommitLogBackup clBackup;

    @Inject
    public CommitLogBackupTask(
            IConfiguration config,
            Provider<AbstractBackupPath> pathFactory,
            CommitLogBackup clBackup,
            IFileSystemContext backupFileSystemCtx) {
        super(config, backupFileSystemCtx, pathFactory);
        this.clBackup = clBackup;
    }

    @Override
    public void execute() throws Exception {
        try {
            logger.debug("Checking for any archived commitlogs");
            // double-check the permission
            if (config.isBackingUpCommitLogs())
                clBackup.upload(config.getCommitLogBackupRestoreFromDirs(), null);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static TaskTimer getTimer(IConfiguration config) {
        return new SimpleTimer(JOBNAME, 60L * 1000); // every 1 min
    }

    public static void addObserver(IMessageObserver observer) {
        observers.add(observer);
    }

    public static void removeObserver(IMessageObserver observer) {
        observers.remove(observer);
    }

    public void notifyObservers() {
        for (IMessageObserver observer : observers) {
            if (observer != null) {
                logger.debug("Updating CL observers now ...");
                observer.update(BACKUP_MESSAGE_TYPE.COMMITLOG, clRemotePaths);
            } else logger.info("Observer is Null, hence can not notify ...");
        }
    }

    @Override
    protected void processColumnFamily(String keyspace, String columnFamily, File backupDir)
            throws Exception {
        // Do nothing.
    }

    @Override
    protected void addToRemotePath(String remotePath) {
        clRemotePaths.add(remotePath);
    }
}
