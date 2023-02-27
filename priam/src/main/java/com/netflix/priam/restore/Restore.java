/*
 * Copyright 2013 Netflix, Inc.
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
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.MetaData;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.Sleeper;
import java.nio.file.Path;
import java.util.concurrent.Future;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main class for restoring data from backup. Backup restored using this way are not encrypted. */
@Singleton
public class Restore extends AbstractRestore {
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);

    @Inject
    public Restore(
            IConfiguration config,
            @Named("backup") IBackupFileSystem fs,
            Sleeper sleeper,
            ICassandraProcess cassProcess,
            Provider<AbstractBackupPath> pathProvider,
            InstanceIdentity instanceIdentity,
            RestoreTokenSelector tokenSelector,
            MetaData metaData,
            InstanceState instanceState,
            IPostRestoreHook postRestoreHook) {
        super(
                config,
                fs,
                JOBNAME,
                sleeper,
                pathProvider,
                instanceIdentity,
                tokenSelector,
                cassProcess,
                metaData,
                instanceState,
                postRestoreHook);
    }

    @Override
    protected final Future<Path> downloadFile(final AbstractBackupPath path) throws Exception {
        return fs.asyncDownloadFile(path, 5 /* retries */);
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }

    @Override
    public String getName() {
        return JOBNAME;
    }
}
