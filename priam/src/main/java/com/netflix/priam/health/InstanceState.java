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

import com.google.inject.Singleton;
import com.netflix.priam.backup.Status;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Contains the state of the health of processed managed by Priam, and
 * maintains the isHealthy flag used for reporting discovery health check.
 * <p>
 * Created by aagrawal on 9/19/17.
 */
@Singleton
public class InstanceState {
    public enum NODE_STATE {
        JOIN, //This state to be used when Priam is joining the ring for the first time or was already assigned this token.
        REPLACE //This state to be used when Priam replaces an instance from the token range.
    }

    //Bootstrap status
    private final AtomicBoolean isBootstrapping = new AtomicBoolean(false);
    private NODE_STATE nodeState;
    private long bootstrapTime;

    //Cassandra process status
    private final AtomicBoolean isCassandraProcessAlive = new AtomicBoolean(false);
    private final AtomicBoolean isGossipActive = new AtomicBoolean(false);
    private final AtomicBoolean isThriftActive = new AtomicBoolean(false);
    private final AtomicBoolean isNativeTransportActive = new AtomicBoolean(false);
    private final AtomicBoolean isRequiredDirectoriesExist = new AtomicBoolean(false);
    private final AtomicBoolean isYmlWritten = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(false);

    //Backup status
    private Status backupStatus = null;
    private Date lastSuccessfulBackupTime;

    //Restore status
    private Status restoreStatus = null;
    private Date restoreTime;

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("InstanceState{");
        sb.append("isHealthy=").append(isHealthy);

        sb.append(", isBootstrapping=").append(isBootstrapping);
        sb.append(", nodeState=").append(nodeState);
        sb.append(", bootstrapTime=").append(bootstrapTime);

        sb.append(", isCassandraProcessAlive=").append(isCassandraProcessAlive);
        sb.append(", isGossipActive=").append(isGossipActive);
        sb.append(", isThriftActive=").append(isThriftActive);
        sb.append(", isNativeTransportActive=").append(isNativeTransportActive);
        sb.append(", isRequiredDirectoriesExist=").append(isRequiredDirectoriesExist);
        sb.append(", isYmlWritten=").append(isYmlWritten);

        sb.append(", backupStatus=").append(backupStatus);
        sb.append(", lastSuccessfulBackupTime=").append(lastSuccessfulBackupTime);

        sb.append(", restoreStatus=").append(restoreStatus);
        sb.append(", restoreTime=").append(restoreTime);
        sb.append('}');
        return sb.toString();
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

    public long getBootstrapTime() {
        return bootstrapTime;
    }

    public void setBootstrapTime(DateTime bootstrapTime) {
        this.bootstrapTime = bootstrapTime.getMillis();
    }

    public void setNodeState(NODE_STATE nodeState) {
        this.nodeState = nodeState;
    }

    /* Backup */
    public Status getBackupStatus() {
        return backupStatus;
    }

    public void setBackupStatus(Status backupStatus) {
        this.backupStatus = backupStatus;
    }

    public Date getLastSuccessfulBackupTime() {
        return lastSuccessfulBackupTime;
    }

    public void setLastSuccessfulBackupTime(Date lastSuccessfulBackupTime) {
        this.lastSuccessfulBackupTime = lastSuccessfulBackupTime;
    }

    /* Restore */
    public Status getRestoreStatus() {
        return restoreStatus;
    }

    public void setRestoreStatus(Status restoreStatus) {
        this.restoreStatus = restoreStatus;
    }

    public Date getRestoreTime() {
        return restoreTime;
    }

    public void setRestoreTime(Date restoreTime) {
        this.restoreTime = restoreTime;
    }


    public boolean isHealthy() {
        return isHealthy.get();
    }

    private void setHealthy() {
        this.isHealthy.set(isCassandraProcessAlive() && isRequiredDirectoriesExist() && isGossipActive() && isYmlWritten() && (isThriftActive() || isNativeTransportActive()));
    }

    public boolean isYmlWritten() {
        return this.isYmlWritten.get();
    }

    public void setYmlWritten(boolean yml) {
        this.isYmlWritten.set(yml);
    }

}