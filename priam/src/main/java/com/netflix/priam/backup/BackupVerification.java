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

import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
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
    private BackupVerificationResult latestResult;

    @Inject
    public BackupVerification(
            @Named("v1") IMetaProxy metaV1Proxy,
            @Named("v2") IMetaProxy metaV2Proxy,
            IBackupStatusMgr backupStatusMgr,
            Provider<AbstractBackupPath> abstractBackupPathProvider) {
        this.metaV1Proxy = metaV1Proxy;
        this.metaV2Proxy = metaV2Proxy;
        this.backupStatusMgr = backupStatusMgr;
        this.abstractBackupPathProvider = abstractBackupPathProvider;
    }

    public IMetaProxy getMetaProxy(BackupVersion backupVersion) {
        switch (backupVersion) {
            case SNAPSHOT_BACKUP:
                return metaV1Proxy;
            case SNAPSHOT_META_SERVICE:
                return metaV2Proxy;
        }

        return null;
    }

    public Optional<BackupVerificationResult> verifyLatestBackup(
            BackupVersion backupVersion, boolean force, DateRange dateRange)
            throws IllegalArgumentException {
        IMetaProxy metaProxy = getMetaProxy(backupVersion);
        for (BackupMetadata backupMetadata :
                backupStatusMgr.getLatestBackupMetadata(backupVersion, dateRange)) {
            if (backupMetadata.getLastValidated() == null || force) {
                Optional<BackupVerificationResult> result = verifyBackup(metaProxy, backupMetadata);
                if (result.isPresent()) {
                    return result;
                }
            } else {
                updateLatestResult(backupMetadata);
                return Optional.of(latestResult);
            }
        }
        latestResult = null;
        return Optional.empty();
    }

    public List<BackupMetadata> verifyBackupsInRange(
            BackupVersion backupVersion, DateRange dateRange) throws IllegalArgumentException {
        IMetaProxy metaProxy = getMetaProxy(backupVersion);
        List<BackupMetadata> results = new ArrayList<>();
        for (BackupMetadata backupMetadata :
                backupStatusMgr.getLatestBackupMetadata(backupVersion, dateRange)) {
            if (backupMetadata.getLastValidated() != null
                    || verifyBackup(metaProxy, backupMetadata).isPresent()) {
                results.add(backupMetadata);
            }
        }
        return results;
    }

    /** returns the latest valid backup verification result if we have found one within the SLO * */
    public Optional<Instant> getLatestVerfifiedBackupTime() {
        return latestResult == null ? Optional.empty() : Optional.of(latestResult.snapshotInstant);
    }

    private Optional<BackupVerificationResult> verifyBackup(
            IMetaProxy metaProxy, BackupMetadata latestBackupMetaData) {
        Path metadataLocation = Paths.get(latestBackupMetaData.getSnapshotLocation());
        metadataLocation = metadataLocation.subpath(1, metadataLocation.getNameCount());
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseRemote(metadataLocation.toString());
        BackupVerificationResult result = metaProxy.isMetaFileValid(abstractBackupPath);
        if (result.valid) {
            updateLatestResult(latestBackupMetaData);
            Date now = new Date(DateUtil.getInstant().toEpochMilli());
            latestBackupMetaData.setLastValidated(now);
            backupStatusMgr.update(latestBackupMetaData);
            return Optional.of(result);
        }
        return Optional.empty();
    }

    private void updateLatestResult(BackupMetadata backupMetadata) {
        Instant snapshotInstant = backupMetadata.getStart().toInstant();
        if (latestResult == null || latestResult.snapshotInstant.isBefore(snapshotInstant)) {
            latestResult = new BackupVerificationResult();
            latestResult.valid = true;
            latestResult.manifestAvailable = true;
            latestResult.snapshotInstant = backupMetadata.getStart().toInstant();
            Path snapshotLocation = Paths.get(backupMetadata.getSnapshotLocation());
            latestResult.remotePath =
                    snapshotLocation.subpath(1, snapshotLocation.getNameCount()).toString();
        }
    }
}
