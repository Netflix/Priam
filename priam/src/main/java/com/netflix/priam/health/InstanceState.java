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
package com.netflix.priam.health;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.backup.BackupMetadata;
import com.netflix.priam.backup.Status;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the state of the health of processed managed by Priam, and maintains the isHealthy flag
 * used for reporting discovery health check.
 *
 * <p>Created by aagrawal on 9/19/17.
 */
@Singleton
public class InstanceState {
    private static final Logger logger = LoggerFactory.getLogger(InstanceState.class);

    public enum NODE_STATE {
        // This state to be used when Priam is joining the ring for the first time or was
        // already assigned this token.
        JOIN,
        // This state to be used when Priam replaces an instance from the token range.
        REPLACE
    }

    // Bootstrap status
    private final AtomicBoolean isBootstrapping = new AtomicBoolean(false);
    private NODE_STATE nodeState;
    private LocalDateTime bootstrapTime;

    // Cassandra process status
    private final AtomicBoolean isCassandraProcessAlive = new AtomicBoolean(false);
    private final AtomicBoolean shouldCassandraBeAlive = new AtomicBoolean(false);
    private final AtomicLong lastAttemptedStartTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicBoolean isGossipActive = new AtomicBoolean(false);
    private final AtomicBoolean isThriftActive = new AtomicBoolean(false);
    private final AtomicBoolean isNativeTransportActive = new AtomicBoolean(false);
    private final AtomicBoolean isRequiredDirectoriesExist = new AtomicBoolean(false);
    private final AtomicBoolean isYmlWritten = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(false);
    private final AtomicBoolean isHealthyOverride = new AtomicBoolean(true);

    // Backup status
    private BackupMetadata backupStatus;

    // Restore status
    private final RestoreStatus restoreStatus;

    @Inject
    InstanceState(RestoreStatus restoreStatus) {
        this.restoreStatus = restoreStatus;
    }

    @Override
    public String toString() {
        return GsonJsonSerializer.getGson().toJson(this);
    }

    public boolean isGossipActive() {
        return isGossipActive.get();
    }

    public void setIsGossipActive(boolean isGossipActive) {
        this.isGossipActive.set(isGossipActive);
        setHealthy();
    }

    public boolean isThriftActive() {
        return isThriftActive.get();
    }

    public void setIsThriftActive(boolean isThriftActive) {
        this.isThriftActive.set(isThriftActive);
        setHealthy();
    }

    public boolean isNativeTransportActive() {
        return isNativeTransportActive.get();
    }

    public void setIsNativeTransportActive(boolean isNativeTransportActive) {
        this.isNativeTransportActive.set(isNativeTransportActive);
        setHealthy();
    }

    public boolean isRequiredDirectoriesExist() {
        return isRequiredDirectoriesExist.get();
    }

    public void setIsRequiredDirectoriesExist(boolean isRequiredDirectoriesExist) {
        this.isRequiredDirectoriesExist.set(isRequiredDirectoriesExist);
        setHealthy();
    }

    public boolean isCassandraProcessAlive() {
        return isCassandraProcessAlive.get();
    }

    public void setCassandraProcessAlive(boolean isSideCarProcessAlive) {
        this.isCassandraProcessAlive.set(isSideCarProcessAlive);
        setHealthy();
    }

    public boolean shouldCassandraBeAlive() {
        return shouldCassandraBeAlive.get();
    }

    public void setShouldCassandraBeAlive(boolean shouldCassandraBeAlive) {
        this.shouldCassandraBeAlive.set(shouldCassandraBeAlive);
        setIsHealthyOverride(shouldCassandraBeAlive);
    }

    public void setIsHealthyOverride(boolean isHealthyOverride) {
        this.isHealthyOverride.set(isHealthyOverride);
    }

    public boolean isHealthyOverride() {
        return this.isHealthyOverride.get();
    }

    public void markLastAttemptedStartTime() {
        this.lastAttemptedStartTime.set(System.currentTimeMillis());
    }

    public long getLastAttemptedStartTime() {
        return this.lastAttemptedStartTime.get();
    }

    /* Boostrap */
    public boolean isBootstrapping() {
        return isBootstrapping.get();
    }

    public void setBootstrapping(boolean isBootstrapping) {
        this.isBootstrapping.set(isBootstrapping);
    }

    public NODE_STATE getNodeState() {
        return nodeState;
    }

    public LocalDateTime getBootstrapTime() {
        return bootstrapTime;
    }

    public void setBootstrapTime(LocalDateTime bootstrapTime) {
        this.bootstrapTime = bootstrapTime;
    }

    public void setNodeState(NODE_STATE nodeState) {
        this.nodeState = nodeState;
    }

    /* Backup */
    public BackupMetadata getBackupStatus() {
        return backupStatus;
    }

    public void setBackupStatus(BackupMetadata backupMetadata) {
        this.backupStatus = backupMetadata;
    }

    /* Restore */
    public RestoreStatus getRestoreStatus() {
        return restoreStatus;
    }

    // A dirty way to set restore status. This is required as setting restore status implies health
    // could change.
    public void setRestoreStatus(Status status) {
        restoreStatus.status = status;
        setHealthy();
    }

    public boolean isHealthy() {
        return isHealthy.get();
    }

    private boolean isRestoring() {
        return restoreStatus != null
                && restoreStatus.getStatus() != null
                && restoreStatus.getStatus() == Status.STARTED;
    }

    private void setHealthy() {
        this.isHealthy.set(
                isRestoring()
                        || (isCassandraProcessAlive()
                                && isRequiredDirectoriesExist()
                                && isGossipActive()
                                && isYmlWritten()
                                && isHealthyOverride()
                                && (isThriftActive() || isNativeTransportActive())));
    }

    public boolean isYmlWritten() {
        return this.isYmlWritten.get();
    }

    public void setYmlWritten(boolean yml) {
        this.isYmlWritten.set(yml);
    }

    public static class RestoreStatus {
        private LocalDateTime startDateRange, endDateRange; // Date range to restore from
        private LocalDateTime executionStartTime,
                executionEndTime; // Start-end time of the actual restore execution
        private String snapshotMetaFile; // Location of the snapshot meta file selected for restore.
        // the state of a restore.  Note: this is different than the "status" of a Task.
        private Status status;

        public void resetStatus() {
            this.snapshotMetaFile = null;
            this.status = null;
            this.startDateRange = endDateRange = null;
            this.executionStartTime = this.executionEndTime = null;
        }

        @Override
        public String toString() {
            return GsonJsonSerializer.getGson().toJson(this);
        }

        public Status getStatus() {
            return status;
        }

        public void setStartDateRange(LocalDateTime startDateRange) {
            this.startDateRange = startDateRange;
        }

        public void setEndDateRange(LocalDateTime endDateRange) {
            this.endDateRange = endDateRange;
        }

        public void setExecutionStartTime(LocalDateTime executionStartTime) {
            this.executionStartTime = executionStartTime;
        }

        public void setExecutionEndTime(LocalDateTime executionEndTime) {
            this.executionEndTime = executionEndTime;
        }

        public LocalDateTime getStartDateRange() {
            return startDateRange;
        }

        public LocalDateTime getEndDateRange() {
            return endDateRange;
        }

        public LocalDateTime getExecutionStartTime() {
            return executionStartTime;
        }

        public LocalDateTime getExecutionEndTime() {
            return executionEndTime;
        }

        public String getSnapshotMetaFile() {
            return snapshotMetaFile;
        }

        public void setSnapshotMetaFile(String snapshotMetaFile) {
            this.snapshotMetaFile = snapshotMetaFile;
        }
    }
}
