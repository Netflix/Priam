/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.priam.backup;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.utils.MaxSizeHashMap;
import java.io.*;
import java.util.LinkedList;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation for {@link IBackupStatusMgr}. This will save the snapshot status in local
 * file. Created by aagrawal on 7/11/17.
 */
@Singleton
public class FileSnapshotStatusMgr extends BackupStatusMgr {
    private static final Logger logger = LoggerFactory.getLogger(FileSnapshotStatusMgr.class);
    private static final int IN_MEMORY_SNAPSHOT_CAPACITY = 60;
    private final String filename;

    /**
     * Constructor to initialize the file based snapshot status manager.
     *
     * @param config {@link IConfiguration} of priam to find where file should be saved/read from.
     * @param instanceState Status of the instance encapsulating health and other metadata of Priam
     *     and Cassandra.
     */
    @Inject
    public FileSnapshotStatusMgr(IConfiguration config, InstanceState instanceState) {
        super(
                IN_MEMORY_SNAPSHOT_CAPACITY,
                instanceState); // Fetch capacity from properties, if required.
        this.filename = config.getBackupStatusFileLoc();
        init();
    }

    private void init() {
        // Retrieve entire file and re-populate the list.
        File snapshotFile = new File(filename);
        if (!snapshotFile.exists()) {
            snapshotFile.getParentFile().mkdirs();
            logger.info(
                    "Snapshot status file do not exist on system. Bypassing initilization phase.");
            backupMetadataMap = new MaxSizeHashMap<>(capacity);
            return;
        }

        try (final ObjectInputStream inputStream =
                new ObjectInputStream(new FileInputStream(snapshotFile))) {
            backupMetadataMap = (Map<String, LinkedList<BackupMetadata>>) inputStream.readObject();
            logger.info(
                    "Snapshot status of size {} fetched successfully from {}",
                    backupMetadataMap.size(),
                    filename);
        } catch (IOException e) {
            logger.error(
                    "Error while trying to fetch snapshot status from {}. Error: {}. If this is first time after upgrading Priam, ignore this.",
                    filename,
                    e.getLocalizedMessage());
            e.printStackTrace();
        } catch (Exception e) {
            logger.error(
                    "Error while trying to fetch snapshot status from {}. Error: {}.",
                    filename,
                    e.getLocalizedMessage());
            e.printStackTrace();
        }

        if (backupMetadataMap == null) backupMetadataMap = new MaxSizeHashMap<>(capacity);
    }

    @Override
    public void save(BackupMetadata backupMetadata) {
        File snapshotFile = new File(filename);
        if (!snapshotFile.exists()) snapshotFile.getParentFile().mkdirs();

        // Will save entire list to file.
        try (final ObjectOutputStream out =
                new ObjectOutputStream(new FileOutputStream(filename))) {
            out.writeObject(backupMetadataMap);
            out.flush();
            logger.info(
                    "Snapshot status of size {} is saved to {}",
                    backupMetadataMap.size(),
                    filename);
        } catch (IOException e) {
            logger.error(
                    "Error while trying to persist snapshot status to {}. Error: {}",
                    filename,
                    e.getLocalizedMessage());
        }
    }

    @Override
    public LinkedList<BackupMetadata> fetch(String snapshotDate) {
        // No need to fetch from local machine as it was read once at start. No point reading again
        // and again.
        return backupMetadataMap.get(snapshotDate);
    }
}
