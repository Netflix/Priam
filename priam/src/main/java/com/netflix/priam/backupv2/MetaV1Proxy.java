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

package com.netflix.priam.backupv2;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.FileReader;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 12/18/18. */
public class MetaV1Proxy implements IMetaProxy {
    private static final Logger logger = LoggerFactory.getLogger(MetaV1Proxy.class);
    private final IBackupFileSystem fs;

    @Inject
    MetaV1Proxy(IConfiguration configuration, IFileSystemContext backupFileSystemCtx) {
        fs = backupFileSystemCtx.getFileStrategy(configuration);
    }

    @Override
    public Path getLocalMetaFileDirectory() {
        return null;
    }

    @Override
    public String getMetaPrefix(DateUtil.DateRange dateRange) {
        return null;
    }

    @Override
    public List<AbstractBackupPath> findMetaFiles(DateUtil.DateRange dateRange) {
        Date startTime = new Date(dateRange.getStartTime().toEpochMilli());
        Date endTime = new Date(dateRange.getEndTime().toEpochMilli());
        String restorePrefix = fs.getPrefix().toString();
        logger.debug("Looking for snapshot meta file within restore prefix: {}", restorePrefix);
        List<AbstractBackupPath> metas = Lists.newArrayList();

        Iterator<AbstractBackupPath> backupfiles = fs.list(restorePrefix, startTime, endTime);

        while (backupfiles.hasNext()) {
            AbstractBackupPath path = backupfiles.next();
            if (path.getType() == AbstractBackupPath.BackupFileType.META)
                // Since there are now meta file for incrementals as well as snapshot, we need to
                // find the correct one (i.e. the snapshot meta file (meta.json))
                if (path.getFileName().equalsIgnoreCase("meta.json")) {
                    metas.add(path);
                }
        }

        Collections.sort(metas, Collections.reverseOrder());

        if (metas.size() == 0) {
            logger.info(
                    "No meta v1 file found on remote file system for the time period: {}",
                    dateRange);
        }

        return metas;
    }

    @Override
    public Path downloadMetaFile(AbstractBackupPath meta) throws BackupRestoreException {
        Path localFile = Paths.get(meta.newRestoreFile().getAbsolutePath() + ".download");
        fs.downloadFile(Paths.get(meta.getRemotePath()), localFile, 10);
        return localFile;
    }

    @Override
    public List<String> getSSTFilesFromMeta(Path localMetaPath) throws Exception {
        if (localMetaPath.toFile().isDirectory() || !localMetaPath.toFile().exists())
            throw new InvalidPathException(
                    localMetaPath.toString(), "Input path is either directory or do not exist");

        List<String> result = new ArrayList<>();
        JSONParser jsonParser = new JSONParser();
        org.json.simple.JSONArray fileList =
                (org.json.simple.JSONArray)
                        jsonParser.parse(new FileReader(localMetaPath.toFile()));
        fileList.forEach(entry -> result.add(entry.toString()));
        return result;
    }

    @Override
    public void cleanupOldMetaFiles() {}
}
