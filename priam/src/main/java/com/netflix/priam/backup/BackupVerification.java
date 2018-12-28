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
    private final IMetaProxy metaProxy;
    private final Provider<AbstractBackupPath> abstractBackupPathProvider;

    @Inject
    BackupVerification(
            @Named("v1") IMetaProxy metaProxy,
            Provider<AbstractBackupPath> abstractBackupPathProvider) {
        this.metaProxy = metaProxy;
        this.abstractBackupPathProvider = abstractBackupPathProvider;
    }

    public Optional<BackupMetadata> getLatestBackupMetaData(List<BackupMetadata> metadata) {
        return metadata.stream()
                .filter(backupMetadata -> backupMetadata.getStatus() == Status.FINISHED)
                .sorted(Comparator.comparing(BackupMetadata::getStart).reversed())
                .findFirst();
    }

    public Optional<BackupVerificationResult> verifyBackup(List<BackupMetadata> metadata) {
        if (metadata == null || metadata.isEmpty()) return Optional.empty();

        Optional<BackupMetadata> latestBackupMetaData = getLatestBackupMetaData(metadata);

        if (!latestBackupMetaData.isPresent()) {
            logger.error("No backup found which finished during the time provided.");
            return Optional.empty();
        }

        Path metadataLocation = Paths.get(latestBackupMetaData.get().getSnapshotLocation());
        metadataLocation = metadataLocation.subpath(1, metadataLocation.getNameCount());
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseRemote(metadataLocation.toString());
        return Optional.of((metaProxy.isMetaFileValid(abstractBackupPath)));
    }
}
