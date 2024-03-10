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

package com.netflix.priam.backup;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class FakeBackupFileSystem extends AbstractFileSystem {
    private List<AbstractBackupPath> flist = new ArrayList<>();
    public Set<String> downloadedFiles = new HashSet<>();
    public Set<String> uploadedFiles = new HashSet<>();
    private String baseDir;
    private String region;
    private String clusterName;

    @Inject
    public FakeBackupFileSystem(
            IConfiguration configuration,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr,
            Provider<AbstractBackupPath> pathProvider) {
        super(configuration, backupMetrics, backupNotificationMgr, pathProvider);
    }

    public void setupTest(List<String> files) {
        clearTest();
        for (String file : files) {
            AbstractBackupPath path = pathProvider.get();
            path.parseRemote(file);
            flist.add(path);
        }
    }

    private void clearTest() {
        flist.clear();
        downloadedFiles.clear();
        uploadedFiles.clear();
    }

    public void addFile(String file) {
        AbstractBackupPath path = pathProvider.get();
        path.parseRemote(file);
        flist.add(path);
    }

    @Override
    public Iterator<String> listFileSystem(String prefix, String delimiter, String marker) {
        ArrayList<String> items = new ArrayList<>();
        flist.stream()
                .forEach(
                        abstractBackupPath -> {
                            if (abstractBackupPath.getRemotePath().startsWith(prefix))
                                items.add(abstractBackupPath.getRemotePath());
                        });
        return items.iterator();
    }

    public void shutdown() {
        // nop
    }

    @Override
    public long getFileSize(String remotePath) throws BackupRestoreException {
        return 0;
    }

    @Override
    public boolean doesRemoteFileExist(Path remotePath) {
        for (AbstractBackupPath abstractBackupPath : flist) {
            if (abstractBackupPath.getRemotePath().equalsIgnoreCase(remotePath.toString()))
                return true;
        }
        return false;
    }

    @Override
    public void deleteFiles(List<Path> remotePaths) throws BackupRestoreException {
        remotePaths
                .stream()
                .forEach(
                        remotePath -> {
                            AbstractBackupPath path = pathProvider.get();
                            path.parseRemote(remotePath.toString());
                            flist.remove(path);
                        });
    }

    @Override
    public void cleanup() {
        clearTest();
    }

    @Override
    protected void downloadFileImpl(AbstractBackupPath path, String suffix)
            throws BackupRestoreException {
        if (path.getType() == AbstractBackupPath.BackupFileType.META_V2) {
            Path destination = Paths.get(path.newRestoreFile().getAbsolutePath() + suffix);
            if (!destination.toFile().exists()) {
                Path origin =
                        Paths.get(
                                        "src/test/resources/"
                                                + path.getClusterName()
                                                + "_"
                                                + Paths.get(path.getRemotePath()).getFileName())
                                .toAbsolutePath();
                try {
                    Files.copy(origin, destination);
                } catch (IOException io) {
                    throw new BackupRestoreException(io.getMessage(), io);
                }
            }
        }
        downloadedFiles.add(path.getRemotePath());
    }

    @Override
    protected long uploadFileImpl(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        uploadedFiles.add(path.getBackupFile().getAbsolutePath());
        addFile(path.getRemotePath());
        return path.getBackupFile().length();
    }
}
