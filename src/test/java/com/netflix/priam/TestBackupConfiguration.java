package com.netflix.priam;

import com.google.common.collect.Lists;
import com.netflix.priam.config.BackupConfiguration;

public class TestBackupConfiguration extends BackupConfiguration {

    public TestBackupConfiguration() {
        setRestorePrefix("");
        setS3BaseDir("casstestbackup");
        setS3BucketName("TEST-netflix.platform.S3");
        setCommitLogEnabled(false);
        setCommitLogLocation("cass/backup/cl/");
        setHour(12);
        setBackupThreads(2);
        setRestoreThreads(3);
        setIncrementalEnabled(true);
        setUploadThrottleBytesPerSec(0);
        setRestoreKeyspaces(Lists.<String>newArrayList());
        setChunkSizeMB(5*1024*1024);
        setRestoreClosestToken(false);
        setRetentionDays(5);
        setAvailabilityZonesToBackup(Lists.<String>newArrayList());
        setStreamingThroughputMbps(400);
        setMultiThreadedCompaction(false);
    }

}
