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
import com.netflix.priam.backup.*;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.FileReader;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.*;
import javax.inject.Inject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.iterators.FilterIterator;
import org.apache.commons.io.FileUtils;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 12/18/18. */
public class MetaV1Proxy implements IMetaProxy {
    private static final Logger logger = LoggerFactory.getLogger(MetaV1Proxy.class);
    private final IBackupFileSystem fs;

    @Inject
    public MetaV1Proxy(IConfiguration configuration, IFileSystemContext backupFileSystemCtx) {
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

        metas.sort(Collections.reverseOrder());

        if (metas.size() == 0) {
            logger.info(
                    "No meta v1 file found on remote file system for the time period: {}",
                    dateRange);
        }

        return metas;
    }

    @Override
    public BackupVerificationResult isMetaFileValid(AbstractBackupPath metaBackupPath) {
        BackupVerificationResult result = new BackupVerificationResult();
        result.remotePath = metaBackupPath.getRemotePath();
        result.snapshotInstant = metaBackupPath.getTime().toInstant();

        try {
            // Download the meta file.
            Path metaFile = downloadMetaFile(metaBackupPath);
            // Read the local meta file.
            List<String> metaFileList = getSSTFilesFromMeta(metaFile);
            FileUtils.deleteQuietly(metaFile.toFile());
            result.manifestAvailable = true;

            // List the remote file system to validate the backup.
            String prefix = fs.getPrefix().toString();
            Date strippedMsSnapshotTime =
                    new Date(result.snapshotInstant.truncatedTo(ChronoUnit.MINUTES).toEpochMilli());
            Iterator<AbstractBackupPath> backupfiles =
                    fs.list(prefix, strippedMsSnapshotTime, strippedMsSnapshotTime);

            // Return validation fail if backup filesystem listing failed.
            if (!backupfiles.hasNext()) {
                logger.warn(
                        "ERROR: No files available while doing backup filesystem listing. Declaring the verification failed.");
                return result;
            }

            // Convert the remote listing to String.
            List<String> remoteListing = new ArrayList<>();
            while (backupfiles.hasNext()) {
                AbstractBackupPath path = backupfiles.next();
                if (path.getType() == AbstractBackupPath.BackupFileType.SNAP)
                    remoteListing.add(path.getRemotePath());
            }

            if (metaFileList.isEmpty() && remoteListing.isEmpty()) {
                logger.info(
                        "Uncommon Scenario: Both meta file and backup filesystem listing is empty. Considering this as success");
                result.valid = true;
                return result;
            }

            ArrayList<String> filesMatched =
                    (ArrayList<String>) CollectionUtils.intersection(metaFileList, remoteListing);
            result.filesMatched = filesMatched.size();
            result.filesInMetaOnly = metaFileList;
            result.filesInMetaOnly.removeAll(filesMatched);

            // There could be a scenario that backupfilesystem has more files than meta file. e.g.
            // some leftover objects
            result.valid = (result.filesInMetaOnly.isEmpty());
        } catch (Exception e) {
            logger.error(
                    "Error while processing meta file: " + metaBackupPath, e.getLocalizedMessage());
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public Path downloadMetaFile(AbstractBackupPath meta) throws BackupRestoreException {
        fs.downloadFile(meta, ".download" /* suffix */, 10 /* retries */);
        return Paths.get(meta.newRestoreFile().getAbsolutePath() + ".download");
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
    public Iterator<AbstractBackupPath> getIncrementals(DateUtil.DateRange dateRange) {
        String prefix = fs.getPrefix().toString();
        Iterator<AbstractBackupPath> iterator =
                fs.list(
                        prefix,
                        new Date(dateRange.getStartTime().toEpochMilli()),
                        new Date(dateRange.getEndTime().toEpochMilli()));
        return new FilterIterator<>(
                iterator,
                abstractBackupPath ->
                        abstractBackupPath.getType() == AbstractBackupPath.BackupFileType.SST);
    }

    @Override
    public void cleanupOldMetaFiles() {}
}
