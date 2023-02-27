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

import com.netflix.priam.health.InstanceState;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.MaxSizeHashMap;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A means to manage metadata for various types of backups (snapshots, incrementals)
 */
@Singleton
public abstract class BackupStatusMgr implements IBackupStatusMgr {

    private static final Logger logger = LoggerFactory.getLogger(BackupStatusMgr.class);

    /**
     * Map<yyyymmdd, List<{@link BackupMetadata}>: Map of completed snapshots represented by its
     * snapshot day (yyyymmdd) and a list of snapshots started on that day Note: A {@link
     * LinkedList} was chosen for fastest retrieval of latest snapshot.
     */
    Map<String, LinkedList<BackupMetadata>> backupMetadataMap;

    final int capacity;
    private final InstanceState instanceState;

    /**
     * @param capacity Capacity to hold in-memory snapshot status days.
     * @param instanceState Status of the instance encapsulating health and other metadata of Priam
     *     and Cassandra.
     */
    @Inject
    public BackupStatusMgr(int capacity, InstanceState instanceState) {
        this.capacity = capacity;
        this.instanceState = instanceState;
        // This is to avoid us loading lot of status in memory.
        // We will fetch previous status from backend service, if required.
        backupMetadataMap = new MaxSizeHashMap<>(capacity);
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public Map<String, LinkedList<BackupMetadata>> getAllSnapshotStatus() {
        return backupMetadataMap;
    }

    @Override
    public LinkedList<BackupMetadata> locate(Date snapshotDate) {
        return locate(DateUtil.formatyyyyMMdd(snapshotDate));
    }

    @Override
    public LinkedList<BackupMetadata> locate(String snapshotDate) {
        if (StringUtils.isEmpty(snapshotDate)) return null;

        // See if in memory
        if (backupMetadataMap.containsKey(snapshotDate)) return backupMetadataMap.get(snapshotDate);

        LinkedList<BackupMetadata> metadataLinkedList = fetch(snapshotDate);

        // Save the result in local cache so we don't hit data store/file.
        backupMetadataMap.put(snapshotDate, metadataLinkedList);

        return metadataLinkedList;
    }

    @Override
    public void start(BackupMetadata backupMetadata) {
        LinkedList<BackupMetadata> metadataLinkedList = locate(backupMetadata.getSnapshotDate());

        if (metadataLinkedList == null) {
            metadataLinkedList = new LinkedList<>();
        }

        metadataLinkedList.addFirst(backupMetadata);
        backupMetadataMap.put(backupMetadata.getSnapshotDate(), metadataLinkedList);
        instanceState.setBackupStatus(backupMetadata);
        // Save the backupMetaDataMap
        save(backupMetadata);
    }

    @Override
    public void finish(BackupMetadata backupMetadata) {
        // validate that it has actually finished. If not, then set the status and current date.
        if (backupMetadata.getStatus() != Status.FINISHED)
            backupMetadata.setStatus(Status.FINISHED);

        if (backupMetadata.getCompleted() == null)
            backupMetadata.setCompleted(
                    Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime());

        instanceState.setBackupStatus(backupMetadata);
        update(backupMetadata);
    }

    @Override
    public void update(BackupMetadata backupMetadata) {
        // Retrieve the snapshot metadata and then update the finish date/status.
        retrieveAndUpdate(backupMetadata);

        // Save the backupMetaDataMap
        save(backupMetadata);
    }

    private void retrieveAndUpdate(final BackupMetadata backupMetadata) {
        // Retrieve the snapshot metadata and then update the date/status.
        LinkedList<BackupMetadata> metadataLinkedList = locate(backupMetadata.getSnapshotDate());

        if (metadataLinkedList == null) {
            logger.error(
                    "No previous backupMetaData found. This should not happen. Creating new to ensure app keeps running.");
            metadataLinkedList = new LinkedList<>();
            backupMetadataMap.put(backupMetadata.getSnapshotDate(), metadataLinkedList);
        }

        Optional<BackupMetadata> searchedData =
                metadataLinkedList
                        .stream()
                        .filter(backupMetadata1 -> backupMetadata.equals(backupMetadata1))
                        .findFirst();
        if (!searchedData.isPresent()) {
            metadataLinkedList.addFirst(backupMetadata);
        }
        searchedData.ifPresent(
                backupMetadata1 -> {
                    backupMetadata1.setCompleted(backupMetadata.getCompleted());
                    backupMetadata1.setStatus(backupMetadata.getStatus());
                    backupMetadata1.setCassandraSnapshotSuccess(
                            backupMetadata.isCassandraSnapshotSuccess());
                    backupMetadata1.setSnapshotLocation(backupMetadata.getSnapshotLocation());
                    backupMetadata1.setLastValidated(backupMetadata.getLastValidated());
                });
    }

    @Override
    public void failed(BackupMetadata backupMetadata) {
        // validate that it has actually failed. If not, then set the status and current date.
        if (backupMetadata.getCompleted() == null)
            backupMetadata.setCompleted(
                    Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime());

        // Set this later to ensure the status
        if (backupMetadata.getStatus() != Status.FAILED) backupMetadata.setStatus(Status.FAILED);

        instanceState.setBackupStatus(backupMetadata);
        update(backupMetadata);
    }

    /**
     * Implementation on how to save the backup metadata
     *
     * @param backupMetadata BackupMetadata to be saved
     */
    protected abstract void save(BackupMetadata backupMetadata);

    /**
     * Implementation on how to retrieve the backup metadata(s) for a given date from store.
     *
     * @param snapshotDate Snapshot date to be retrieved from datastore in format of yyyyMMdd
     * @return The list of snapshots started on the snapshot day in descending order of snapshot
     *     start time.
     */
    protected abstract LinkedList<BackupMetadata> fetch(String snapshotDate);

    public List<BackupMetadata> getLatestBackupMetadata(
            BackupVersion backupVersion, DateUtil.DateRange dateRange) {
        Instant startDay = dateRange.getStartTime().truncatedTo(ChronoUnit.DAYS);
        Instant endDay = dateRange.getEndTime().truncatedTo(ChronoUnit.DAYS);

        List<BackupMetadata> allBackups = new ArrayList<>();
        Instant previousDay = endDay;
        do {
            // We need to find the latest backupmetadata in this date range.
            logger.info(
                    "Will try to find snapshot for : {}",
                    DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, previousDay));
            List<BackupMetadata> backupsForDate = locate(new Date(previousDay.toEpochMilli()));
            if (backupsForDate != null) allBackups.addAll(backupsForDate);
            previousDay = previousDay.minus(1, ChronoUnit.DAYS);
        } while (!previousDay.isBefore(startDay));

        // Return all the backups which are FINISHED and were "started" in the dateRange provided.
        // Do not compare the end time of snapshot as it may take random amount of time to finish
        // the snapshot.
        return allBackups
                .stream()
                .filter(Objects::nonNull)
                .filter(backupMetadata -> backupMetadata.getStatus() == Status.FINISHED)
                .filter(backupMetadata -> backupMetadata.getBackupVersion().equals(backupVersion))
                .filter(
                        backupMetadata ->
                                backupMetadata
                                                        .getStart()
                                                        .toInstant()
                                                        .compareTo(dateRange.getStartTime())
                                                >= 0
                                        && backupMetadata
                                                        .getStart()
                                                        .toInstant()
                                                        .compareTo(dateRange.getEndTime())
                                                <= 0)
                .sorted(Comparator.comparing(BackupMetadata::getStart).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "BackupStatusMgr{"
                + "backupMetadataMap="
                + backupMetadataMap
                + ", capacity="
                + capacity
                + '}';
    }
}
