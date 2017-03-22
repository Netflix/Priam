/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.SystemUtils;
import org.apache.commons.collections.CollectionUtils;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by aagrawal on 2/16/17.
 * This class validates the backup by doing listing of files in the backup destination and comparing with meta.json by downloading from the location.
 * Input: BackupMetadata that needs to be verified.
 * Since one backupmetadata can have multiple start time, provide one startTime if interested in verifying one particular backup.
 * Leave startTime as null to get the latest snapshot for the provided BackupMetadata.
 */
@Singleton
public class BackupVerification {

    private static final Logger logger = LoggerFactory.getLogger(BackupVerification.class);
    private final SimpleDateFormat simpleDateFormatDate = new SimpleDateFormat("yyyyMMdd");
    private final SimpleDateFormat simpleDateFormatTime = new SimpleDateFormat("yyyyMMddHHmm");
    private IBackupFileSystem bkpStatusFs;
    private IConfiguration config;

    @Inject
    BackupVerification(@Named("backup_status") IBackupFileSystem bkpStatusFs, IConfiguration config) {
        this.bkpStatusFs = bkpStatusFs;
        this.config = config;
    }

    public BackupVerificationResult verifyBackup(BackupMetadata metadata, Date startTime) {
        BackupVerificationResult result = new BackupVerificationResult();

        if (metadata == null || metadata.getBackups().isEmpty())
            return result;

        result.snapshotAvailable = true;
        result.selectedDate = metadata.getKey();

        logger.info("Snapshots found for : " + metadata.getKey() + " [" + metadata.getBackups() + "]");
        //find the latest date (default) or verify if one provided
        Date latestDate = null;
        for (String backupTime : metadata.getBackups()) {
            Date parsedBackupTime = SystemUtils.getDate(backupTime);
            if (latestDate == null || latestDate.before(parsedBackupTime))
                latestDate = parsedBackupTime;

            if (startTime != null && parsedBackupTime.equals(startTime)) {
                latestDate = startTime;
                break;
            }
        }

        result.snapshotTime = simpleDateFormatTime.format(latestDate);
        logger.info("Latest/Requested snapshot date found: " + simpleDateFormatTime.format(latestDate) + ", for selected/provided date: " + metadata.getKey());

        //Get Backup File Iterator
        String prefix = config.getBackupPrefix();
        logger.info("Looking for meta file in the location:  " + prefix);

        Iterator<AbstractBackupPath> backupfiles = bkpStatusFs.list(prefix, latestDate, latestDate);
        //Return validation fail if backup filesystem listing failed.
        if (!backupfiles.hasNext())
        {
            logger.warn("ERROR: No files available while doing backup filesystem listing. Declaring the verification failed.");
            return result;
        }

        result.backupFileListAvail = true;

        List<AbstractBackupPath> metas = new LinkedList<>();
        List<String> s3Listing = new ArrayList<>();

        while (backupfiles.hasNext()) {
            AbstractBackupPath path = backupfiles.next();
            if (path.getFileName().equalsIgnoreCase("meta.json"))
                metas.add(path);
            else
                s3Listing.add(path.getRemotePath());
        }

        if (metas.size() == 0) {
            logger.error("No meta found for snapshotdate: " + simpleDateFormatTime.format(latestDate));
            return result;
        }

        result.metaFileFound = true;
        //Download meta.json from backup location and uncompress it.
        List<String> metaFileList = new ArrayList<>();
        try {
            bkpStatusFs.download(metas.get(0), new FileOutputStream("tmp_meta.json"));
            logger.info("Meta file successfully downloaded to localhost: " + metas.get(0));

            JSONParser jsonParser = new JSONParser();
            org.json.simple.JSONArray fileList = (org.json.simple.JSONArray) jsonParser.parse(new FileReader("tmp_meta.json"));
            for (int i = 0; i < fileList.size(); i++)
                metaFileList.add(fileList.get(i).toString());

        } catch (Exception e) {
            logger.error("Error while fetching meta.json from path: " + metas.get(0), e);
            return result;
        }

        if (metaFileList.isEmpty() && s3Listing.isEmpty()) {
            logger.info("Uncommon Scenario: Both meta file and backup filesystem listing is empty. Considering this as success");
            result.valid = true;
            return result;
        }

        //Atleast meta file or s3 listing contains some file.
        result.filesInS3Only = new ArrayList<>(s3Listing);
        result.filesInS3Only.removeAll(metaFileList);
        result.filesInMetaOnly = new ArrayList<>(metaFileList);
        result.filesInMetaOnly.removeAll(s3Listing);
        result.filesMatched = (ArrayList<String>) CollectionUtils.intersection(metaFileList, s3Listing);

        //There could be a scenario that backupfilesystem has more files than meta file. e.g. some leftover objects
        if (result.filesInMetaOnly.size() == 0)
            result.valid = true;

        return result;
    }
}
