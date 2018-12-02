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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.json.simple.JSONArray;

@Singleton
public class FakeBackupFileSystem extends AbstractFileSystem {
    private List<AbstractBackupPath> flist;
    public Set<String> downloadedFiles;
    public Set<String> uploadedFiles;
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
        flist = new ArrayList<>();
        for (String file : files) {
            AbstractBackupPath path = pathProvider.get();
            path.parseRemote(file);
            flist.add(path);
        }
        downloadedFiles = new HashSet<>();
        uploadedFiles = new HashSet<>();
    }

    public void setupTest() {
        clearTest();
        flist = new ArrayList<>();
        downloadedFiles = new HashSet<>();
        uploadedFiles = new HashSet<>();
    }

    private void clearTest() {
        if (flist != null) flist.clear();
        if (downloadedFiles != null) downloadedFiles.clear();
    }

    public void addFile(String file) {
        AbstractBackupPath path = pathProvider.get();
        path.parseRemote(file);
        flist.add(path);
    }

    @Override
    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till) {
        String[] paths = bucket.split(String.valueOf(RemoteBackupPath.PATH_SEP));

        if (paths.length > 1) {
            baseDir = paths[1];
            region = paths[2];
            clusterName = paths[3];
        }

        List<AbstractBackupPath> tmpList = new ArrayList<>();
        for (AbstractBackupPath path : flist) {

            if ((path.time.after(start) && path.time.before(till))
                    || path.time.equals(start)
                            && path.baseDir.equals(baseDir)
                            && path.clusterName.equals(clusterName)
                            && path.region.equals(region)) {
                tmpList.add(path);
            }
        }
        return tmpList.iterator();
    }

    @Override
    public Iterator<String> list(String prefix, String delimiter) {
        return new TransformIterator<>(flist.iterator(), AbstractBackupPath::getRemotePath);
    }

    public void shutdown() {
        // nop
    }

    @Override
    public long getFileSize(Path remotePath) throws BackupRestoreException {
        return 0;
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    protected void downloadFileImpl(Path remotePath, Path localPath) throws BackupRestoreException {
        {
            // List all files and generate the file
            try (FileWriter fr = new FileWriter(localPath.toFile())) {
                JSONArray jsonObj = new JSONArray();
                for (AbstractBackupPath filePath : flist) {
                    if (filePath.type == BackupFileType.SNAP) {
                        jsonObj.add(filePath.getRemotePath());
                    }
                }
                fr.write(jsonObj.toJSONString());
                fr.flush();
            } catch (IOException io) {
                throw new BackupRestoreException(io.getMessage(), io);
            }
        }
        downloadedFiles.add(remotePath.toString());
        System.out.println("Downloading " + remotePath.toString());
    }

    @Override
    protected long uploadFileImpl(Path localPath, Path remotePath) throws BackupRestoreException {
        uploadedFiles.add(localPath.toFile().getAbsolutePath());
        return localPath.toFile().length();
    }
}
