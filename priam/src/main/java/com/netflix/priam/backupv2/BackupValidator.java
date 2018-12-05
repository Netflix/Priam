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

import com.google.inject.Provider;
import com.netflix.priam.backup.*;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes a meta file and verifies that all the components listed in that meta file are
 * successfully uploaded to remote file system. Created by aagrawal on 11/28/18.
 */
public class BackupValidator {
    private static final Logger logger = LoggerFactory.getLogger(BackupVerification.class);
    private final IBackupFileSystem fs;
    private final Provider<AbstractBackupPath> abstractBackupPathProvider;
    private boolean isBackupValid;

    @Inject
    public BackupValidator(
            IConfiguration configuration,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> abstractBackupPathProvider) {
        fs = backupFileSystemCtx.getFileStrategy(configuration);
        this.abstractBackupPathProvider = abstractBackupPathProvider;
    }

    /**
     * Fetch the list of all META_V2 files on the remote file system for the provided valid
     * daterange.
     *
     * @param dateRange the time period to scan in the remote file system for meta files.
     * @return List of all the META_V2 files from the remote file system.
     */
    public List<AbstractBackupPath> findMetaFiles(DateUtil.DateRange dateRange) {
        ArrayList<AbstractBackupPath> metas = new ArrayList<>();
        String prefix = getMetaPrefix(dateRange);
        String marker = getMetaPrefix(new DateUtil.DateRange(dateRange.getStartTime(), null));
        logger.info(
                "Listing filesystem with prefix: {}, marker: {}, daterange: {}",
                prefix,
                marker,
                dateRange);
        Iterator<String> iterator = fs.listFileSystem(prefix, null, marker);

        while (iterator.hasNext()) {
            AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
            abstractBackupPath.parseRemote(iterator.next());
            logger.debug("Meta file found: {}", abstractBackupPath);
            if (abstractBackupPath.getLastModified().toEpochMilli()
                            >= dateRange.getStartTime().toEpochMilli()
                    && abstractBackupPath.getLastModified().toEpochMilli()
                            <= dateRange.getEndTime().toEpochMilli()) {
                metas.add(abstractBackupPath);
            }
        }

        Collections.sort(metas, Collections.reverseOrder());

        if (metas.size() == 0) {
            logger.info(
                    "No meta file found on remote file system for the time period: {}", dateRange);
        }

        return metas;
    }

    /**
     * Find the latest valid meta file in a given valid time range. It will return the handle to the
     * file via AbstractBackupPath object.
     *
     * @param dateRange the time period to scan in the remote file system for meta files.
     * @return the AbstractBackupPath denoting the "local" file which is valid or null. Caller needs
     *     to delete the file.
     * @throws BackupRestoreException if there is issue contacting remote file system, fetching the
     *     file etc.
     */
    public AbstractBackupPath findLatestValidMetaFile(DateUtil.DateRange dateRange)
            throws BackupRestoreException {
        List<AbstractBackupPath> metas = findMetaFiles(dateRange);
        logger.info("Meta files found: {}", metas);

        for (AbstractBackupPath meta : metas) {
            Path localFile = Paths.get(meta.newRestoreFile().getAbsolutePath());
            fs.downloadFile(Paths.get(meta.getRemotePath()), localFile, 10);
            boolean isValid = isMetaFileValid(localFile);
            logger.info("Meta: {}, isValid: {}", meta, isValid);
            if (!isValid) FileUtils.deleteQuietly(localFile.toFile());
            else return meta;
        }

        return null;
    }

    private String getMetaPrefix(DateUtil.DateRange dateRange) {
        Path location = fs.getPrefix();
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        return abstractBackupPath
                .remoteV2Prefix(location, dateRange, AbstractBackupPath.BackupFileType.META_V2)
                .toString();
    }

    /**
     * Validate that all the files mentioned in the meta file actually exists on remote file system.
     *
     * @param metaFile Path to the local uncompressed/unencrypted meta file
     * @return true if all the files mentioned in meta file are present on remote file system. It
     *     will return false in case of any error.
     */
    public boolean isMetaFileValid(Path metaFile) {
        try {
            isBackupValid = true;
            new MetaFileBackupValidator().readMeta(metaFile);
        } catch (FileNotFoundException fne) {
            isBackupValid = false;
            logger.error(fne.getLocalizedMessage());
        } catch (IOException ioe) {
            isBackupValid = false;
            logger.error(
                    "IO Error while processing meta file: " + metaFile, ioe.getLocalizedMessage());
            ioe.printStackTrace();
        }
        return isBackupValid;
    }

    private class MetaFileBackupValidator extends MetaFileReader {
        @Override
        public void process(ColumnfamilyResult columnfamilyResult) {
            for (ColumnfamilyResult.SSTableResult ssTableResult :
                    columnfamilyResult.getSstables()) {
                for (FileUploadResult fileUploadResult : ssTableResult.getSstableComponents()) {
                    if (!isBackupValid) {
                        break;
                    }

                    try {
                        isBackupValid =
                                isBackupValid
                                        && fs.doesRemoteFileExist(
                                                Paths.get(fileUploadResult.getBackupPath()));
                    } catch (BackupRestoreException e) {
                        // For any error, mark that file is not available.
                        isBackupValid = false;
                        break;
                    }
                }
            }
        }
    }
}
