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
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.FileReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by aagrawal on 2/16/17. This class validates the backup by doing listing of files in the
 * backup destination and comparing with meta.json by downloading from the location. Input:
 * BackupMetadata that needs to be verified. Since one backupmetadata can have multiple start time,
 * provide one startTime if interested in verifying one particular backup. Leave startTime as null
 * to get the latest snapshot for the provided BackupMetadata.
 */
@Singleton
public class BackupVerification {

    private static final Logger logger = LoggerFactory.getLogger(BackupVerification.class);
    private final IBackupFileSystem bkpStatusFs;
    private final IConfiguration config;

    @Inject
    BackupVerification(@Named("backup") IBackupFileSystem bkpStatusFs, IConfiguration config) {
        this.bkpStatusFs = bkpStatusFs;
        this.config = config;
    }

    public BackupVerificationResult verifyBackup(List<BackupMetadata> metadata, Date startTime) {
        BackupVerificationResult result = new BackupVerificationResult();

        if (metadata == null || metadata.isEmpty()) return result;

        result.snapshotAvailable = true;
        // All the dates should be same.
        result.selectedDate = metadata.get(0).getSnapshotDate();

        List<String> backups =
                metadata.stream()
                        .map(
                                backupMetadata ->
                                        DateUtil.formatyyyyMMddHHmm(backupMetadata.getStart()))
                        .collect(Collectors.toList());
        logger.info("Snapshots found for {} : [{}]", result.selectedDate, backups);

        // find the latest date (default) or verify if one provided
        Date latestDate = null;
        for (BackupMetadata backupMetadata : metadata) {
            if (latestDate == null || latestDate.before(backupMetadata.getStart()))
                latestDate = backupMetadata.getStart();

            if (startTime != null
                    && DateUtil.formatyyyyMMddHHmm(backupMetadata.getStart())
                            .equals(DateUtil.formatyyyyMMddHHmm(startTime))) {
                latestDate = startTime;
                break;
            }
        }

        result.snapshotTime = DateUtil.formatyyyyMMddHHmm(latestDate);
        logger.info(
                "Latest/Requested snapshot date found: {}, for selected/provided date: {}",
                result.snapshotTime,
                result.selectedDate);

        // Get Backup File Iterator
        String prefix = config.getBackupPrefix();
        logger.info("Looking for meta file in the location:  {}", prefix);

        Date strippedMsSnapshotTime = DateUtil.getDate(result.snapshotTime);
        Iterator<AbstractBackupPath> backupfiles =
                bkpStatusFs.list(prefix, strippedMsSnapshotTime, strippedMsSnapshotTime);
        // Return validation fail if backup filesystem listing failed.
        if (!backupfiles.hasNext()) {
            logger.warn(
                    "ERROR: No files available while doing backup filesystem listing. Declaring the verification failed.");
            return result;
        }

        result.backupFileListAvail = true;

        List<AbstractBackupPath> metas = new LinkedList<>();
        List<String> s3Listing = new ArrayList<>();

        while (backupfiles.hasNext()) {
            AbstractBackupPath path = backupfiles.next();
            if (path.getFileName().equalsIgnoreCase("meta.json")) metas.add(path);
            else s3Listing.add(path.getRemotePath());
        }

        if (metas.size() == 0) {
            logger.error(
                    "No meta found for snapshotdate: {}", DateUtil.formatyyyyMMddHHmm(latestDate));
            return result;
        }

        result.metaFileFound = true;
        // Download meta.json from backup location and uncompress it.
        List<String> metaFileList = new ArrayList<>();
        try {
            Path metaFileLocation =
                    FileSystems.getDefault().getPath(config.getDataFileLocation(), "tmp_meta.json");
            bkpStatusFs.downloadFile(Paths.get(metas.get(0).getRemotePath()), metaFileLocation, 5);
            logger.info(
                    "Meta file successfully downloaded to localhost: {}",
                    metaFileLocation.toString());

            JSONParser jsonParser = new JSONParser();
            org.json.simple.JSONArray fileList =
                    (org.json.simple.JSONArray)
                            jsonParser.parse(new FileReader(metaFileLocation.toFile()));
            for (Object aFileList : fileList) metaFileList.add(aFileList.toString());

        } catch (Exception e) {
            logger.error("Error while fetching meta.json from path: {}", metas.get(0), e);
            return result;
        }

        if (metaFileList.isEmpty() && s3Listing.isEmpty()) {
            logger.info(
                    "Uncommon Scenario: Both meta file and backup filesystem listing is empty. Considering this as success");
            result.valid = true;
            return result;
        }

        // Atleast meta file or s3 listing contains some file.
        result.filesInS3Only = new ArrayList<>(s3Listing);
        result.filesInS3Only.removeAll(metaFileList);
        result.filesInMetaOnly = new ArrayList<>(metaFileList);
        result.filesInMetaOnly.removeAll(s3Listing);
        result.filesMatched =
                (ArrayList<String>) CollectionUtils.intersection(metaFileList, s3Listing);

        // There could be a scenario that backupfilesystem has more files than meta file. e.g. some
        // leftover objects
        if (result.filesInMetaOnly.size() == 0) result.valid = true;

        return result;
    }
}
