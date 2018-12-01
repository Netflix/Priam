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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

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
    public long getFileSize(Path remotePath) throws BackupRestoreException {
        return 0;
    }

    @Override
    public Iterator<String> list(String prefix, String delimiter) {
        return Collections.emptyIterator();
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    protected void downloadFileImpl(Path remotePath, Path localPath)
            throws BackupRestoreException {}

    @Override
    protected long uploadFileImpl(Path localPath, Path remotePath) throws BackupRestoreException {
        return 0;
    }
}
