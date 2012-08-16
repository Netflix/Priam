package com.netflix.priam.config;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class BackupConfiguration {
    @JsonProperty
    private String autoRestoreSnapshotName;

    @JsonProperty
    private int chunkSizeMB;

    @JsonProperty
    private int hour;

    @JsonProperty
    private List<String> availabilityZonesToBackup;

    @JsonProperty
    private int retentionDays;

    @JsonProperty
    private int backupThreads;

    @JsonProperty
    private int restoreThreads;

    @JsonProperty
    private String s3BucketName;

    @JsonProperty
    private String s3BaseDir;

    @JsonProperty
    private boolean commitLogEnabled;

    @JsonProperty
    private String commitLogLocation;

    @JsonProperty
    private boolean incrementalEnabled;

    @JsonProperty
    private boolean multiThreadedCompaction;

    @JsonProperty
    private boolean restoreClosestToken;

    @JsonProperty
    private List<String> restoreKeyspaces;

    @JsonProperty
    private String restorePrefix;

    @JsonProperty
    private int streamingThroughputMbps;

    @JsonProperty
    private long uploadThrottleBytesPerSec;


    public String getAutoRestoreSnapshotName() {
        return autoRestoreSnapshotName;
    }

    public long getChunkSizeMB() {
        return chunkSizeMB * 1024L * 1024L;
    }

    public int getHour() {
        return hour;
    }

    public List<String> getAvailabilityZonesToBackup() {
        return availabilityZonesToBackup;
    }

    public int getRetentionDays() {
        return retentionDays;
    }

    public int getBackupThreads() {
        return backupThreads;
    }

    public int getRestoreThreads() {
        return restoreThreads;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getS3BaseDir() {
        return s3BaseDir;
    }

    public boolean isCommitLogEnabled() {
        return commitLogEnabled;
    }

    public String getCommitLogLocation() {
        return commitLogLocation;
    }

    public boolean isIncrementalEnabled() {
        return incrementalEnabled;
    }

    public boolean isMultiThreadedCompaction() {
        return multiThreadedCompaction;
    }

    public boolean isRestoreClosestToken() {
        return restoreClosestToken;
    }

    public List<String> getRestoreKeyspaces() {
        return restoreKeyspaces;
    }

    public String getRestorePrefix() {
        return restorePrefix;
    }

    public int getStreamingThroughputMbps() {
        return streamingThroughputMbps;
    }

    public long getUploadThrottleBytesPerSec() {
        return uploadThrottleBytesPerSec;
    }

    public void setAutoRestoreSnapshotName(String autoRestoreSnapshotName) {
        this.autoRestoreSnapshotName = autoRestoreSnapshotName;
    }

    public void setChunkSizeMB(int chunkSizeMB) {
        this.chunkSizeMB = chunkSizeMB;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public void setAvailabilityZonesToBackup(List<String> availabilityZonesToBackup) {
        this.availabilityZonesToBackup = availabilityZonesToBackup;
    }

    public void setRetentionDays(int retentionDays) {
        this.retentionDays = retentionDays;
    }

    public void setBackupThreads(int backupThreads) {
        this.backupThreads = backupThreads;
    }

    public void setRestoreThreads(int restoreThreads) {
        this.restoreThreads = restoreThreads;
    }

    public void setS3BucketName(String s3BucketName) {
        this.s3BucketName = s3BucketName;
    }

    public void setS3BaseDir(String s3BaseDir) {
        this.s3BaseDir = s3BaseDir;
    }

    public void setCommitLogEnabled(boolean commitLogEnabled) {
        this.commitLogEnabled = commitLogEnabled;
    }

    public void setCommitLogLocation(String commitLogLocation) {
        this.commitLogLocation = commitLogLocation;
    }

    public void setIncrementalEnabled(boolean incrementalEnabled) {
        this.incrementalEnabled = incrementalEnabled;
    }

    public void setMultiThreadedCompaction(boolean multiThreadedCompaction) {
        this.multiThreadedCompaction = multiThreadedCompaction;
    }

    public void setRestoreClosestToken(boolean restoreClosestToken) {
        this.restoreClosestToken = restoreClosestToken;
    }

    public void setRestoreKeyspaces(List<String> restoreKeyspaces) {
        this.restoreKeyspaces = restoreKeyspaces;
    }

    public void setRestorePrefix(String restorePrefix) {
        this.restorePrefix = restorePrefix;
    }

    public void setStreamingThroughputMbps(int streamingThroughputMbps) {
        this.streamingThroughputMbps = streamingThroughputMbps;
    }

    public void setUploadThrottleBytesPerSec(long uploadThrottleBytesPerSec) {
        this.uploadThrottleBytesPerSec = uploadThrottleBytesPerSec;
    }

}
