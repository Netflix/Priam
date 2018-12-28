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

import com.netflix.priam.backup.*;
import com.netflix.priam.utils.DateUtil;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes a meta file and verifies that all the components listed in that meta file are
 * successfully uploaded to remote file system. Created by aagrawal on 11/28/18.
 */
public class BackupValidator {
    private static final Logger logger = LoggerFactory.getLogger(BackupVerification.class);
    private IMetaProxy metaProxy;

    @Inject
    public BackupValidator(@Named("v2") IMetaProxy metaProxy) {
        this.metaProxy = metaProxy;
    }

    /**
     * Find the latest valid meta file in a given valid time range. It will return the handle to the
     * file via AbstractBackupPath object.
     *
     * @param dateRange the time period to scan in the remote file system for meta files.
     * @return the BackupVerificationResult containing the details of the valid meta file. If none
     *     is found, null is returned.
     * @throws BackupRestoreException if there is issue contacting remote file system, fetching the
     *     file etc.
     */
    public Optional<BackupVerificationResult> findLatestValidMetaFile(DateUtil.DateRange dateRange)
            throws BackupRestoreException {
        List<AbstractBackupPath> metas = metaProxy.findMetaFiles(dateRange);
        logger.info("Meta files found: {}", metas);

        for (AbstractBackupPath meta : metas) {
            BackupVerificationResult result = metaProxy.isMetaFileValid(meta);
            logger.info("BackupVerificationResult: {}", result);
            if (result.valid) return Optional.of(result);
        }

        return Optional.empty();
    }
}
