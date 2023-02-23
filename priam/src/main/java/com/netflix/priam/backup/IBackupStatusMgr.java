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

import com.google.inject.ImplementedBy;
import com.netflix.priam.utils.DateUtil;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This will store the status of snapshots as they start, fail or finish. By default they will save
 * the snapshot status of last 60 days on instance. Created by aagrawal on 1/30/17.
 */
@ImplementedBy(FileSnapshotStatusMgr.class)
public interface IBackupStatusMgr {
    /**
     * Return the list of snapshot executed on provided day or null if not present.
     *
     * @param snapshotDate date on which snapshot was started.
     * @return List of snapshots started on that day in descending order of snapshot start time.
     */
    List<BackupMetadata> locate(Date snapshotDate);

    /**
     * Return the list of snapshot executed on provided day or null if not present.
     *
     * @param snapshotDate date on which snapshot was started in the format of yyyyMMdd or
     *     yyyyMMddHHmm.
     * @return List of snapshots started on that day in descending order of snapshot start time.
     */
    List<BackupMetadata> locate(String snapshotDate);

    /**
     * Save the status of snapshot BackupMetadata which started in-memory and other implementations,
     * if any.
     *
     * @param backupMetadata backupmetadata that started
     */
    void start(BackupMetadata backupMetadata);

    /**
     * Save the status of successfully finished snapshot BackupMetadata in-memory and other
     * implementations, if any.
     *
     * @param backupMetadata backupmetadata that finished successfully
     */
    void finish(BackupMetadata backupMetadata);

    /**
     * Save the status of failed backupmetadata in-memory and other implementations, if any.
     *
     * @param backupMetadata backupmetadata that failed
     */
    void failed(BackupMetadata backupMetadata);

    /**
     * Update the backup information of backupmetadata in-memory and other implementations, if any.
     *
     * @param backupMetadata backupmetadata to be updated.
     */
    void update(BackupMetadata backupMetadata);

    /**
     * Get the capacity of in-memory status map holding the snapshot status.
     *
     * @return capacity of in-memory snapshot status map.
     */
    int getCapacity();

    /**
     * Get the entire map of snapshot status hold in-memory
     *
     * @return The map of snapshot status in-memory in format. Key is snapshot day in format of
     *     yyyyMMdd (start date of snapshot) with a list of snapshots in the descending order of
     *     snapshot start time.
     */
    Map<String, LinkedList<BackupMetadata>> getAllSnapshotStatus();

    /**
     * Get the list of backup metadata which are finished and have started in the daterange
     * provided, in reverse chronological order of start date.
     *
     * @param backupVersion backup version of the backups to search.
     * @param dateRange time period in which snapshot should have started. Finish time may be after
     *     the endTime in input.
     * @return list of backup metadata which satisfies the input criteria
     */
    List<BackupMetadata> getLatestBackupMetadata(
            BackupVersion backupVersion, DateUtil.DateRange dateRange);
}
