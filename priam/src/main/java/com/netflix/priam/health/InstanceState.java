package com.netflix.priam.health;

import com.google.inject.Singleton;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Contains the state of the health of processed managed by Priam, and
 * maintains the isHealthy flag used for reporting discovery health check.
 *
 * Created by aagrawal on 9/19/17.
 */
@Singleton
public class InstanceState {
    private final AtomicBoolean isSideCarProcessAlive = new AtomicBoolean(false);
    private final AtomicBoolean isBootstrapping = new AtomicBoolean(false);
    private final AtomicBoolean firstBootstrap = new AtomicBoolean(true);
    private final AtomicBoolean isBackup = new AtomicBoolean(false);
    private final AtomicBoolean isBackupSuccessful = new AtomicBoolean(false);
    private final AtomicBoolean isRestore = new AtomicBoolean(false);
    private final AtomicBoolean isRestoreSuccessful = new AtomicBoolean(false);
    private final AtomicBoolean isGossipActive = new AtomicBoolean(false);
    private final AtomicBoolean isThriftActive = new AtomicBoolean(false);
    private final AtomicBoolean isNativeTransportActive = new AtomicBoolean(false);
    private final AtomicBoolean isRequiredDirectoriesExist = new AtomicBoolean(false);
    private final AtomicBoolean isYmlWritten = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(false);
    private long bootstrapTime;
    private long backupTime;
    private long restoreTime;

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("InstanceState{");
        sb.append("isHealthy=").append(isHealthy);

        sb.append(", isBootstrapping=").append(isBootstrapping);
        sb.append(", firstBootstrap=").append(firstBootstrap);
        sb.append(", bootstrapTime=").append(bootstrapTime);

        sb.append(", isSideCarProcessAlive=").append(isSideCarProcessAlive);
        sb.append(", isGossipActive=").append(isGossipActive);
        sb.append(", isThriftActive=").append(isThriftActive);
        sb.append(", isNativeTransportActive=").append(isNativeTransportActive);
        sb.append(", isRequiredDirectoriesExist=").append(isRequiredDirectoriesExist);

        sb.append(", isBackup=").append(isBackup);
        sb.append(", isBackupSuccessful=").append(isBackupSuccessful);
        sb.append(", backupTime=").append(backupTime);

        sb.append(", isRestore=").append(isRestore);
        sb.append(", isRestoreSuccessful=").append(isRestoreSuccessful);
        sb.append(", restoreTime=").append(restoreTime);

        sb.append(", isYmlWritten=").append(isYmlWritten);
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

    public boolean isSideCarProcessAlive() {
        return isSideCarProcessAlive.get();
    }

    public void setSideCarProcessAlive(boolean isSideCarProcessAlive) {
        this.isSideCarProcessAlive.set(isSideCarProcessAlive);
        setHealthy();
    }

    /* Boostrap */
    public boolean isBootstrapping() {
        return isBootstrapping.get();
    }

    public void setBootstrapping(boolean isBootstrapping) {
        this.isBootstrapping.set(isBootstrapping);
    }

    public boolean firstBootstrap() {
        return firstBootstrap.get();
    }

    public long getBootstrapTime() {
        return bootstrapTime;
    }

    public void setBootstrapTime(DateTime bootstrapTime) {
        this.bootstrapTime = bootstrapTime.getMillis();
    }

    public void setFirstBootstrap(boolean firstBootstrap) {
        this.firstBootstrap.set(firstBootstrap);
    }

    /* Backup */
    public boolean isBackingup() {
        return isBackup.get();
    }

    public void setBackingup(boolean isBackup) {
        this.isBackup.set(isBackup);
    }

    public boolean isBackupSuccessful() {
        return isBackupSuccessful.get();
    }

    public long getBackupTime() {
        return backupTime;
    }

    public void setBackupTime(DateTime backupTime) {
        this.backupTime = backupTime.getMillis();
    }

    public void setBackUpStatus(boolean isBackupSuccessful) {
        this.isBackupSuccessful.set(isBackupSuccessful);
    }

    /* Restore */
    public boolean isRestoring() {
        return isRestore.get();
    }

    public void setRestoring(boolean isRestoring) {
        this.isRestore.set(isRestoring);
    }

    public boolean isRestoreSuccessful() {
        return isRestoreSuccessful.get();
    }

    public long getRestoreTime() {
        return restoreTime;
    }

    public void setRestoreTime(DateTime restoreTime) {
        this.restoreTime = restoreTime.getMillis();
    }

    public void setRestoreStatus(boolean isRestoreSuccessful) {
        this.isRestoreSuccessful.set(isRestoreSuccessful);
    }

    public boolean isHealthy() {
        return isHealthy.get();
    }

    private void setHealthy() {
        this.isHealthy.set(isSideCarProcessAlive() && isRequiredDirectoriesExist() && isGossipActive() && (isThriftActive() || isNativeTransportActive()));
    }

    public boolean getYmlWritten() {
        return this.isYmlWritten.get();
    }

    public void setYmlWritten(boolean yml) {
        this.isYmlWritten.set(yml);
    }

}