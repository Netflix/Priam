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
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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
    private final IMetaProxy metaV1Proxy;
    private final IMetaProxy metaV2Proxy;
    private final IBackupStatusMgr backupStatusMgr;
    private final Provider<AbstractBackupPath> abstractBackupPathProvider;

    @Inject
    BackupVerification(
            @Named("v1") IMetaProxy metaV1Proxy,
            @Named("v2") IMetaProxy metaV2Proxy,
            IBackupStatusMgr backupStatusMgr,
            Provider<AbstractBackupPath> abstractBackupPathProvider) {
        this.metaV1Proxy = metaV1Proxy;
        this.metaV2Proxy = metaV2Proxy;
        this.backupStatusMgr = backupStatusMgr;
        this.abstractBackupPathProvider = abstractBackupPathProvider;
    }

    private IMetaProxy getMetaProxy(BackupVersion backupVersion) {
        switch (backupVersion) {
            case SNAPSHOT_BACKUP:
                return metaV1Proxy;
            case SNAPSHOT_META_SERVICE:
                return metaV2Proxy;
        }

        return null;
    }

    public Optional<BackupVerificationResult> verifyBackup(
            BackupVersion backupVersion, boolean force, DateRange dateRange)
            throws UnsupportedTypeException, IllegalArgumentException {
        IMetaProxy metaProxy = getMetaProxy(backupVersion);
        if (metaProxy == null) {
            throw new UnsupportedTypeException(
                    "BackupVersion type: " + backupVersion + " is not supported");
        }

        if (dateRange == null) {
            throw new IllegalArgumentException("dateRange provided is null");
        }

        List<BackupMetadata> metadata =
                backupStatusMgr.getLatestBackupMetadata(backupVersion, dateRange);
        if (metadata == null || metadata.isEmpty()) return Optional.empty();
        for (BackupMetadata backupMetadata : metadata) {
            if (backupMetadata.getLastValidated() != null && !force) {
                // Backup is already validated. Nothing to do.
                BackupVerificationResult result = new BackupVerificationResult();
                result.valid = true;
                result.manifestAvailable = true;
                result.snapshotInstant = backupMetadata.getStart().toInstant();
                Path snapshotLocation = Paths.get(backupMetadata.getSnapshotLocation());
                result.remotePath =
                        snapshotLocation.subpath(1, snapshotLocation.getNameCount()).toString();
                return Optional.of(result);
            }
            BackupVerificationResult backupVerificationResult =
                    verifyBackup(metaProxy, backupMetadata);
            if (logger.isDebugEnabled())
                logger.debug(
                        "BackupVerification: metadata: {}, result: {}",
                        backupMetadata,
                        backupVerificationResult);
            if (backupVerificationResult.valid) {
                backupMetadata.setLastValidated(new Date(DateUtil.getInstant().toEpochMilli()));
                backupStatusMgr.update(backupMetadata);
                return Optional.of(backupVerificationResult);
            }
        }
        return Optional.empty();
    }

    private BackupVerificationResult verifyBackup(
            IMetaProxy metaProxy, BackupMetadata latestBackupMetaData) {
        Path metadataLocation = Paths.get(latestBackupMetaData.getSnapshotLocation());
        metadataLocation = metadataLocation.subpath(1, metadataLocation.getNameCount());
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseRemote(metadataLocation.toString());
        return metaProxy.isMetaFileValid(abstractBackupPath);
    }
}
