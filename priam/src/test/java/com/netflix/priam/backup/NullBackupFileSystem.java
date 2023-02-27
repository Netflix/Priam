/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.priam.backup;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Provider;

public class NullBackupFileSystem extends AbstractFileSystem {

    @Inject
    public NullBackupFileSystem(
            IConfiguration configuration,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr,
            Provider<AbstractBackupPath> pathProvider) {
        super(configuration, backupMetrics, backupNotificationMgr, pathProvider);
    }

    public void shutdown() {
        // NOP
    }

    @Override
    public long getFileSize(String remotePath) throws BackupRestoreException {
        return 0;
    }

    @Override
    public void deleteFiles(List<Path> remotePaths) throws BackupRestoreException {
        // Do nothing.
    }

    @Override
    public Iterator<String> listFileSystem(String prefix, String delimiter, String marker) {
        return Collections.emptyIterator();
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    protected void downloadFileImpl(AbstractBackupPath path, String suffix)
            throws BackupRestoreException {}

    @Override
    protected boolean doesRemoteFileExist(Path remotePath) {
        return false;
    }

    @Override
    protected long uploadFileImpl(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        return 0;
    }
}
