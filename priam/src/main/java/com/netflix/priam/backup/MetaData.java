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
package com.netflix.priam.backup;

import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to create a meta data file with a list of snapshot files. Also list the contents of a meta
 * data file.
 */
public class MetaData {
    private static final Logger logger = LoggerFactory.getLogger(MetaData.class);
    private final Provider<AbstractBackupPath> pathFactory;
    private final List<String> metaRemotePaths = new ArrayList<>();
    private final IBackupFileSystem fs;

    @Inject
    public MetaData(
            Provider<AbstractBackupPath> pathFactory,
            IFileSystemContext backupFileSystemCtx,
            IConfiguration config) {

        this.pathFactory = pathFactory;
        this.fs = backupFileSystemCtx.getFileStrategy(config);
    }

    public AbstractBackupPath set(List<AbstractBackupPath> bps, String snapshotName)
            throws Exception {
        File metafile = createTmpMetaFile();
        try (FileWriter fr = new FileWriter(metafile)) {
            JSONArray jsonObj = new JSONArray();
            for (AbstractBackupPath filePath : bps) jsonObj.add(filePath.getRemotePath());
            fr.write(jsonObj.toJSONString());
        }
        AbstractBackupPath backupfile = decorateMetaJson(metafile, snapshotName);
        fs.uploadAndDelete(backupfile, false /* async */);
        addToRemotePath(backupfile.getRemotePath());
        return backupfile;
    }

    /*
    From the meta.json to be created, populate its meta data for the backup file.
     */
    public AbstractBackupPath decorateMetaJson(File metafile, String snapshotName)
            throws ParseException {
        AbstractBackupPath backupfile = pathFactory.get();
        backupfile.parseLocal(metafile, BackupFileType.META);
        backupfile.setTime(DateUtil.getDate(snapshotName));
        return backupfile;
    }

    /*
     * Determines the existence of the backup meta file.  This meta file could be snapshot (meta.json) or
     * incrementals (meta_keyspace_cf..json).
     *
     * @param backup meta file to search
     * @return true if backup meta file exist, false otherwise.
     */
    public Boolean doesExist(final AbstractBackupPath meta) {
        try {
            fs.downloadFile(meta, "" /* suffix */, 5 /* retries */);
        } catch (Exception e) {
            logger.error("Error downloading the Meta data try with a different date...", e);
        }

        return meta.newRestoreFile().exists();
    }

    public File createTmpMetaFile() throws IOException {
        File metafile = File.createTempFile("meta", ".json");
        File destFile = new File(metafile.getParent(), "meta.json");
        if (destFile.exists()) destFile.delete();
        FileUtils.moveFile(metafile, destFile);
        return destFile;
    }

    private void addToRemotePath(String remotePath) {
        metaRemotePaths.add(remotePath);
    }
}
