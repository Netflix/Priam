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
import com.netflix.priam.utils.DateUtil;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Contains the state of the health of processed managed by Priam, and
 * maintains the isHealthy flag used for reporting discovery health check.
 * <p>
 * Created by aagrawal on 9/19/17.
 */
@Singleton
public class InstanceState {
    private static final Logger logger = LoggerFactory.getLogger(InstanceState.class);

    public enum NODE_STATE {
        JOIN, //This state to be used when Priam is joining the ring for the first time or was already assigned this token.
        REPLACE //This state to be used when Priam replaces an instance from the token range.
    }

    //Bootstrap status
    private final AtomicBoolean isBootstrapping = new AtomicBoolean(false);
    private NODE_STATE nodeState;
    private LocalDateTime bootstrapTime;

    //Cassandra process status
    private final AtomicBoolean isCassandraProcessAlive = new AtomicBoolean(false);
    private final AtomicBoolean isGossipActive = new AtomicBoolean(false);
    private final AtomicBoolean isThriftActive = new AtomicBoolean(false);
    private final AtomicBoolean isNativeTransportActive = new AtomicBoolean(false);
    private final AtomicBoolean isRequiredDirectoriesExist = new AtomicBoolean(false);
    private final AtomicBoolean isYmlWritten = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(false);

    //Backup status
    private BackupMetadata backupMetadata;

    //Restore status
    private RestoreStatus restoreStatus;

    @Inject
    InstanceState(RestoreStatus restoreStatus){
        this.restoreStatus = restoreStatus;
    }

    @Override
    public String toString() {
        try {
            JSONObject object = new JSONObject();
            object.put("isHealthy", isHealthy.get());
            object.put("isBootstrapping", isBootstrapping.get());
            object.put("nodeState", nodeState);
            object.put("bootstrapTime", DateUtil.formatyyyyMMddHHmm(bootstrapTime));
            object.put("isCassandraProcessAlive", isCassandraProcessAlive.get());
            object.put("isGossipActive", isGossipActive.get());
            object.put("isThriftActive", isThriftActive.get());
            object.put("isNativeTransportActive", isNativeTransportActive.get());
            object.put("isRequiredDirectoriesExist", isRequiredDirectoriesExist.get());
            object.put("isYamlWritten", isYmlWritten.get());

            if (backupMetadata != null)
                object.put("backupStatus", backupMetadata.getJSON());
            else object.put("backupStatus", backupMetadata);

            object.put("restoreStatus", restoreStatus.getJSON());
            return object.toString();
        }catch (JSONException ex)
        {
            logger.error("JSONException during toString representation of InstanceState.", ex);
        }
        return null;
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
        return backupMetadata;
    }

    public void setBackupStatus(BackupMetadata backupMetadata) {
        this.backupMetadata = backupMetadata;
    }

    /* Restore */
    public RestoreStatus getRestoreStatus() {
        return restoreStatus;
    }

    // A dirty way to set restore status. This is required as setting restore status implies health could change.
    public void setRestoreStatus(Status status){
        restoreStatus.status = status;
        setHealthy();
    }

    public boolean isHealthy() {
        return isHealthy.get();
    }

    private boolean isRestoring(){
        return restoreStatus != null && restoreStatus.getStatus() != null && restoreStatus.getStatus() == Status.STARTED;
    }
    private void setHealthy() {
        this.isHealthy.set(isRestoring() || (isCassandraProcessAlive() && isRequiredDirectoriesExist() && isGossipActive() && isYmlWritten() && (isThriftActive() || isNativeTransportActive())));
    }

    public boolean isYmlWritten() {
        return this.isYmlWritten.get();
    }

    public void setYmlWritten(boolean yml) {
        this.isYmlWritten.set(yml);
    }

    public static class RestoreStatus {
        private LocalDateTime startDateRange, endDateRange; //Date range to restore from
        private LocalDateTime execStartTime, execEndTime; //Start-end time of the actual restore execution
        private String snapshotMetaFile; //Location of the snapshot meta file selected for restore.
        private Status status;  //the state of a restore.  Note: this is different than the "status" of a Task.

        public void resetStatus(){
            this.snapshotMetaFile = null;
            this.status = null;
            this.startDateRange = endDateRange = null;
            this.execStartTime = this.execEndTime = null;
        }

        @Override
        public String toString() {
            return (getJSON() == null) ? null : getJSON().toString();
        }

        public JSONObject getJSON(){
            try {
                JSONObject object = new JSONObject();
                object.put("startDateRange", DateUtil.formatyyyyMMddHHmm(startDateRange));
                object.put("endDateRange", DateUtil.formatyyyyMMddHHmm(endDateRange));
                object.put("executionStartTime", DateUtil.formatyyyyMMddHHmm(execStartTime));
                object.put("executionEndTime", DateUtil.formatyyyyMMddHHmm(execEndTime));
                object.put("snapshotMetaFile", snapshotMetaFile);
                object.put("status", status.toString());
                return object;
            }catch (JSONException ex)
            {
                logger.error("JSONException during toString representation of RestoreStatus.", ex);
            }
            return null;
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

        public void setExecStartTime(LocalDateTime execStartTime) {
            this.execStartTime = execStartTime;
        }

        public void setExecEndTime(LocalDateTime execEndTime) {
            this.execEndTime = execEndTime;
        }

        public LocalDateTime getStartDateRange() {
            return startDateRange;
        }

        public LocalDateTime getEndDateRange() {
            return endDateRange;
        }

        public LocalDateTime getExecStartTime() {
            return execStartTime;
        }

        public LocalDateTime getExecEndTime() {
            return execEndTime;
        }

        public String getSnapshotMetaFile() {
            return snapshotMetaFile;
        }

        public void setSnapshotMetaFile(String snapshotMetaFile) {
            this.snapshotMetaFile = snapshotMetaFile;
        }
    }
}