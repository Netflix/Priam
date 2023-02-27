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

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import java.io.File;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Provide this to be run as a Quart job
@Singleton
public class CommitLogBackupTask extends AbstractBackup {
    public static final String JOBNAME = "CommitLogBackup";

    private static final Logger logger = LoggerFactory.getLogger(CommitLogBackupTask.class);
    private final CommitLogBackup clBackup;

    @Inject
    public CommitLogBackupTask(IConfiguration config, CommitLogBackup clBackup) {
        super(config);
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
        if (config.isBackingUpCommitLogs())
            return new SimpleTimer(JOBNAME, 60L * 1000); // every 1 min
        else return null;
    }

    @Override
    protected void processColumnFamily(File columnFamilyDirectory) {
        // Do nothing.
    }
}
