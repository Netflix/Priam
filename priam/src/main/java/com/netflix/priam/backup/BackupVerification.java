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
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by aagrawal on 2/16/17. This class validates the backup by doing listing of files in the
 * backup destination and comparing with meta.json by downloading from the location. Input:
 * BackupMetadata that needs to be verified.
 */
@Singleton
public class BackupVerification {

    private static final Logger logger = LoggerFactory.getLogger(BackupVerification.class);
    private final IBackupFileSystem bkpStatusFs;
    private final IConfiguration config;
    private final IMetaProxy metaProxy;
    private final Provider<AbstractBackupPath> abstractBackupPathProvider;

    @Inject
    BackupVerification(
            @Named("backup") IBackupFileSystem bkpStatusFs,
            IConfiguration config,
            @Named("v1") IMetaProxy metaProxy,
            Provider<AbstractBackupPath> abstractBackupPathProvider) {
        this.bkpStatusFs = bkpStatusFs;
        this.config = config;
        this.metaProxy = metaProxy;
        this.abstractBackupPathProvider = abstractBackupPathProvider;
    }

    public Optional<BackupMetadata> getLatestBackupMetaData(List<BackupMetadata> metadata) {
        metadata =
                metadata.stream()
                        .filter(backupMetadata -> backupMetadata.getStatus() == Status.FINISHED)
                        .collect(Collectors.toList());
        metadata.sort((o1, o2) -> o2.getStart().compareTo(o1.getStart()));
        return metadata.stream().findFirst();
    }

    public BackupVerificationResult verifyBackup(List<BackupMetadata> metadata) throws Exception {
        BackupVerificationResult result = new BackupVerificationResult();

        if (metadata == null || metadata.isEmpty()) return result;

        result.snapshotAvailable = true;
        // All the dates should be same.
        result.selectedDate = metadata.get(0).getSnapshotDate();

        Optional<BackupMetadata> latestBackupMetaData = getLatestBackupMetaData(metadata);

        if (!latestBackupMetaData.isPresent()) {
            logger.error("No backup found which finished during the time provided.");
            return result;
        }

        result.snapshotTime = DateUtil.formatyyyyMMddHHmm(latestBackupMetaData.get().getStart());
        logger.info(
                "Latest/Requested snapshot date found: {}, for selected/provided date: {}",
                result.snapshotTime,
                result.selectedDate);

        // Get Backup File Iterator
        String prefix = config.getBackupPrefix();
        Date strippedMsSnapshotTime = DateUtil.getDate(result.snapshotTime);
        Iterator<AbstractBackupPath> backupfiles =
                bkpStatusFs.list(prefix, strippedMsSnapshotTime, strippedMsSnapshotTime);

        // Return validation fail if backup filesystem listing failed.
        if (!backupfiles.hasNext()) {
            logger.warn(
                    "ERROR: No files available while doing backup filesystem listing. Declaring the verification failed.");
            return result;
        }

        // Do remote file listing
        result.backupFileListAvail = true;
        List<AbstractBackupPath> metas = new LinkedList<>();
        List<String> remoteListing = new ArrayList<>();

        while (backupfiles.hasNext()) {
            AbstractBackupPath path = backupfiles.next();
            if (path.getType() == AbstractBackupPath.BackupFileType.META) metas.add(path);
            else remoteListing.add(path.getRemotePath());
        }

        if (metas.size() == 0) {
            logger.error(
                    "Manifest file not found on remote file system for: {}", result.snapshotTime);
            return result;
        }

        result.metaFileFound = true;

        // Download meta.json from backup location.
        Path localMetaPath = metaProxy.downloadMetaFile(metas.get(0));
        List<String> metaFileList = metaProxy.getSSTFilesFromMeta(localMetaPath);
        FileUtils.deleteQuietly(localMetaPath.toFile());

        if (metaFileList.isEmpty() && remoteListing.isEmpty()) {
            logger.info(
                    "Uncommon Scenario: Both meta file and backup filesystem listing is empty. Considering this as success");
            result.valid = true;
            return result;
        }

        // Atleast meta file or s3 listing contains some file.

        result.filesMatched =
                (ArrayList<String>) CollectionUtils.intersection(metaFileList, remoteListing);
        result.filesInS3Only = remoteListing;
        result.filesInS3Only.removeAll(result.filesMatched);
        result.filesInMetaOnly = metaFileList;
        result.filesInMetaOnly.removeAll(result.filesMatched);

        // There could be a scenario that backupfilesystem has more files than meta file. e.g. some
        // leftover objects
        if (result.filesInMetaOnly.size() == 0) result.valid = true;

        return result;
    }
}
