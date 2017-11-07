/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam;

import com.google.inject.ImplementedBy;
import com.netflix.priam.tuner.JVMOption;
import com.netflix.priam.defaultimpl.PriamConfiguration;
import com.netflix.priam.tuner.GCType;
import com.netflix.priam.scheduler.SchedulerType;
import com.netflix.priam.scheduler.UnsupportedTypeException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for Priam's configuration
 */
@ImplementedBy(PriamConfiguration.class)
public interface IConfiguration {

    public void intialize();

    /**
     * @return Path to the home dir of Cassandra
     */
    public String getCassHome();

    public String getYamlLocation();

    /**
     * @return Path to jvm.options file. This is used to pass JVM options to Cassandra.
     */
    public String getJVMOptionsFileLocation();

    /**
     * @return Type of garbage collection mechanism to use for Cassandra. Supported values are CMS,G1GC
     */
    public GCType getGCType() throws UnsupportedTypeException;

    /**
     * @return Set of JVM options to exclude/comment.
     */
    public Map<String, JVMOption> getJVMExcludeSet();

    /**
     * @return Set of JMV options to add/upsert
     */
    public Map<String, JVMOption> getJVMUpsertSet();

    /**
     * @return Path to Cassandra startup script
     */
    public String getCassStartupScript();

    /**
     * @return Path to Cassandra stop sript
     */
    public String getCassStopScript();

    /**
     * Eg: 'my_backup' will result in all files stored under this dir/prefix
     *
     * @return Prefix that will be added to remote backup location
     */
    public String getBackupLocation();

    /**
     * @return Get Backup retention in days
     */
    public int getBackupRetentionDays();

    /**
     * @return Get list of racs to backup. Backup all racs if empty
     */
    public List<String> getBackupRacs();

    /**
     * Bucket name in case of AWS
     *
     * @return Bucket name used for backups
     */
    public String getBackupPrefix();

    /**
     * Location containing backup files. Typically bucket name followed by path
     * to the clusters backup
     */
    public String getRestorePrefix();

    /**
     * @param prefix Set the current restore prefix
     */
    public void setRestorePrefix(String prefix);

    /**
     * @return List of keyspaces to restore. If none, all keyspaces are
     * restored.
     */
    public List<String> getRestoreKeySpaces();

    /**
     * @return Location of the local data dir
     */
    public String getDataFileLocation();

    /**
     * Path where cassandra logs should be stored. This is passed to Cassandra as where to store logs.
     * @return Path to cassandra logs.
     */
    String getLogDirLocation();

    /**
     * @return Location of the hints data directory
     */
    public String getHintsLocation();
    /**
     * @return Location of local cache
     */
    public String getCacheLocation();

    /**
     * @return Location of local commit log dir
     */
    public String getCommitLogLocation();

    /**
     * @return Remote commit log location for backups
     */
    public String getBackupCommitLogLocation();

    /**
     * @return Preferred data part size for multi part uploads
     */
    public long getBackupChunkSize();

    /**
     * @return Cassandra's JMX port
     */
    public int getJmxPort();

    /**
     * @return Enables Remote JMX connections n C*
     */
    public boolean enableRemoteJMX();


    /**
     * Cassandra storage/cluster communication port
     */
    public int getStoragePort();

    public int getSSLStoragePort();

    /**
     * @return Cassandra's thrift port
     */
    public int getThriftPort();

    /**
     * @return Port for CQL binary transport.
     */
    public int getNativeTransportPort();

    /**
     * @return Snitch to be used in cassandra.yaml
     */
    public String getSnitch();

    /**
     * @return Cluster name
     */
    public String getAppName();

    /**
     * @return RAC (or zone for AWS)
     */
    public String getRac();

    /**
     * @return List of all RAC used for the cluster
     */
    public List<String> getRacs();

    /**
     * @return Local hostmame
     */
    public String getHostname();

    /**
     * @return Get instance name (for AWS)
     */
    public String getInstanceName();

    /**
     * @return Max heap size be used for Cassandra
     */
    public String getHeapSize();

    /**
     * @return New heap size for Cassandra
     */
    public String getHeapNewSize();

    /**
     * @return Backup hour for snapshot backups (0 - 23)
     */
    public int getBackupHour();

    /**
     * Cron expression to be used for snapshot backups.
     *
     * @return Backup cron expression for snapshots
     * @see <a href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    public String getBackupCronExpression();

    /**
     * Backup scheduler type to use for backup.
     *
     * @return Type of scheduler to use for backup.  Note the default is TIMER based i.e. to use {@link #getBackupHour()}.
     * If value of "CRON" is provided it starts using {@link #getBackupCronExpression()}.
     */
    public SchedulerType getBackupSchedulerType() throws UnsupportedTypeException;

    /*
     * @return key spaces, comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    public String getSnapshotKeyspaceFilters();

    /*
     * Column Family(ies), comma delimited, to filter from backup.
     * *Note:  the expected format is keyspace.cfname  
     * 
     * @return Column Family(ies), comma delimited, to filter from backup.  If no filter is applied, returns null.
     */
    public String getSnapshotCFFilter();

    /*
     * @return key spaces, comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    public String getIncrementalKeyspaceFilters();

    /*
     * Column Family(ies), comma delimited, to filter from backup.
     * *Note:  the expected format is keyspace.cfname  
     * 
     * @return Column Family(ies), comma delimited, to filter from backup.  If no filter is applied, returns null.
     */
    public String getIncrementalCFFilter();

    /*
     * @return key spaces, comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    public String getRestoreKeyspaceFilter();

    /*
     * Column Family(ies), comma delimited, to filter from backup.
     * Note:  the expected format is keyspace.cfname  
     * 
     * @return Column Family(ies), comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    public String getRestoreCFFilter();

    /**
     * Specifies the start and end time used for restoring data (yyyyMMddHHmm
     * format) Eg: 201201132030,201201142030
     *
     * @return Snapshot to be searched and restored
     */
    public String getRestoreSnapshot();

    /**
     * @return Get the Data Center name (or region for AWS)
     */
    public String getDC();

    /**
     * @param region Set the current data center
     */
    public void setDC(String region);

    /**
     * @return true if it is a multi regional cluster
     */
    public boolean isMultiDC();

    /**
     * @return Number of backup threads for uploading
     */
    public int getMaxBackupUploadThreads();

    /**
     * @return Number of download threads
     */
    public int getMaxBackupDownloadThreads();

    /**
     * @return true if restore should search for nearest token if current token
     * is not found
     */
    public boolean isRestoreClosestToken();

    /**
     * Amazon specific setting to query ASG Membership
     */
    public String getASGName();

    /**
     * Amazon specific setting to query Additional/ Sibling ASG Memberships in csv format to consider while calculating RAC membership
     */
    public String getSiblingASGNames();

    /**
     * Get the security group associated with nodes in this cluster
     */
    public String getACLGroupName();

    /**
     * @return true if incremental backups are enabled
     */
    boolean isIncrBackup();

    /**
     * @return Get host IP
     */
    public String getHostIP();

    /**
     * @return Bytes per second to throttle for backups
     */
    public int getUploadThrottle();

    /**
     * @return true if Priam should local config file for tokens and seeds
     */
    boolean isLocalBootstrapEnabled();

    /**
     * @return In memory compaction limit
     */
    public int getInMemoryCompactionLimit();

    /**
     * @return Compaction throughput
     */
    public int getCompactionThroughput();

    /**
     * @return compaction_throughput_mb_per_sec
     */
    public int getMaxHintWindowInMS();

    /**
     * @return hinted_handoff_throttle_in_kb
     */
    public int getHintedHandoffThrottleKb();

    /**
     * @return max_hints_delivery_threads
     */
    public int getMaxHintThreads();

    /**
     * @return Size of Cassandra max direct memory
     */
    public String getMaxDirectMemory();

    /**
     * @return Bootstrap cluster name (depends on another cass cluster)
     */
    public String getBootClusterName();

    /**
     * @return Get the name of seed provider
     */
    public String getSeedProviderName();

    /**
     * @return Get Memtable throughput settings
     */
    public int getMemtableTotalSpaceMB();

    /**
     * @return memtable_cleanup_threshold in C* yaml
     */
    double getMemtableCleanupThreshold();

    /**
     * @return stream_throughput_outbound_megabits_per_sec in yaml
     */
    public int getStreamingThroughputMB();

    /**
     * @return multithreaded_compaction in yaml
     */
    public boolean getMultithreadedCompaction();

    /**
     * Get the paritioner for this cassandra cluster/node.
     *
     * @return the fully-qualified name of the partitioner class
     */
    public String getPartitioner();

    /**
     * Support for c* 1.1 global key cache size
     */
    public String getKeyCacheSizeInMB();

    /**
     * Support for limiting the total number of keys in c* 1.1 global key cache.
     */
    public String getKeyCacheKeysToSave();

    /**
     * Support for c* 1.1 global row cache size
     */
    public String getRowCacheSizeInMB();

    /**
     * Support for limiting the total number of rows in c* 1.1 global row cache.
     */
    public String getRowCacheKeysToSave();

    /**
     * @return C* Process Name
     */
    public String getCassProcessName();

    /**
     * Defaults to 'allow all'.
     */
    public String getAuthenticator();

    /**
     * Defaults to 'allow all'.
     */
    public String getAuthorizer();

    /**
     * This can be used during cluster migration.
     * When on Target Cluster, keyspace name is different
     * than the original one.
     *
     * @return New Keyspace Name on Target Cluster
     */
    public String getTargetKSName();

    /**
     * This can be used during cluster migration.
     * When on Target Cluster, Column Family name is different
     * than the original one.
     *
     * @return New Column Family Name on Target Cluster
     */
    public String getTargetCFName();

    /**
     * @return true/false, if Cassandra needs to be started manually
     */
    public boolean doesCassandraStartManually();

    /**
     * @return possible values: all, dc, none
     */
    public String getInternodeCompression();

    /**
     * Enable/disable backup/restore of commit logs.
     * @return boolean value true if commit log backup/restore is enabled, false otherwise. Default: false.
     */
    public boolean isBackingUpCommitLogs();

    public String getCommitLogBackupPropsFile();

    public String getCommitLogBackupArchiveCmd();

    public String getCommitLogBackupRestoreCmd();

    public String getCommitLogBackupRestoreFromDirs();

    public String getCommitLogBackupRestorePointInTime();

    public int maxCommitLogsRestore();

    /**
     * @return true/false, if Cassandra is running in a VPC environment
     */
    public boolean isVpcRing();

    public void setRestoreKeySpaces(List<String> keyspaces);

    public boolean isClientSslEnabled();

    public String getInternodeEncryption();

    public boolean isDynamicSnitchEnabled();

    public boolean isThriftEnabled();

    public boolean isNativeTransportEnabled();

    public int getConcurrentReadsCnt();

    public int getConcurrentWritesCnt();

    public int getConcurrentCompactorsCnt();

    public String getRpcServerType();

    public int getRpcMinThreads();

    public int getRpcMaxThreads();

    public int getIndexInterval();

    /*
     * @return the warning threshold in MB's for large partitions encountered during compaction.
     * Default value of 100 is used (default from cassandra.yaml)
     */
    public int getCompactionLargePartitionWarnThresholdInMB();

    public String getExtraConfigParams();

    public String getCassYamlVal(String priamKey);

    public boolean getAutoBoostrap();

    //if using with Datastax Enterprise
    public String getDseClusterType();

    public boolean isCreateNewTokenEnable();

    /*
     * @return the location on disk of the private key used by the cryptography algorithm
     */
    public String getPrivateKeyLocation();

    /*
     * @return the type of source for the restore.  Valid values are: AWSCROSSACCT or GOOGLE.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality.
     * 
     * AWSCROSSACCT
     * - You are restoring from an AWS account which requires cross account assumption where an IAM user in one account is allowed to access resources that belong
     * to a different account.
     * 
     * GOOGLE
     * - You are restoring from Google Cloud Storage
     * 
     */
    public String getRestoreSourceType();

    /*
     * @return true to enable encryption of backup (snapshots, incrementals, commit logs).
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality. 
     */
    public boolean isEncryptBackupEnabled();

    /**
     * Data that needs to be restored is encrypted?
     * @return true if data that needs to be restored is encrypted. Note that setting this value does not play any role until {@link #getRestoreSnapshot()} is set to a non-null value.
     */
    public boolean isRestoreEncrypted();

    /*
     * @return the Amazon Resource Name (ARN).  This is applicable when restoring from an AWS account which requires cross account assumption. 
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality.
     */
    public String getAWSRoleAssumptionArn();

    /*
     * @return Google Cloud Storage service account id to be use within the restore functionality.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality.
     */
    public String getGcsServiceAccountId();

    /*
     * @return the absolute path on disk for the Google Cloud Storage PFX file (i.e. the combined format of the private key and certificate).  
     * This information is to be use within the restore functionality.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality.
     */
    public String getGcsServiceAccountPrivateKeyLoc();

    /*
     * @return the pass phrase use by PGP cryptography.  This information is to be use within the restore and backup functionality when encryption is enabled.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality. 
     */
    public String getPgpPasswordPhrase();

    /*
     * @return public key use by PGP cryptography.  This information is to be use within the restore and backup functionality when encryption is enabled.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality. 
     */
    public String getPgpPublicKeyLoc();

    /**
     * Use this method for adding extra/ dynamic cassandra startup options or env properties
     *
     * @return
     */
    Map<String, String> getExtraEnvParams();

    /*
     * @return the vpc id of the running instance.
     */
    public String getVpcId();

    /*
     * @return the Amazon Resource Name (ARN) for EC2 classic. 
     */
    public String getClassicEC2RoleAssumptionArn();

    /*
     * @return the Amazon Resource Name (ARN) for VPC. 
     */
    public String getVpcEC2RoleAssumptionArn();

    /*
     * @return if the dual account support
     */
    public boolean isDualAccount();

    public Boolean isIncrBackupParallelEnabled();

    /*
     * The number of workers for parallel uploads.
     */
    public int getIncrementalBkupMaxConsumers();

    /*
     * The max number of files queued to be uploaded.
     */
    public int getUncrementalBkupQueueSize();

    /**
     * @return tombstone_warn_threshold in C* yaml
     */
    int getTombstoneWarnThreshold();

    /**
     * @return tombstone_failure_threshold in C* yaml
     */
    int getTombstoneFailureThreshold();

    /**
     * @return streaming_socket_timeout_in_ms in C* yaml
     */
    int getStreamingSocketTimeoutInMS();

    /**
     * List of keyspaces to flush. Default: all keyspaces.
     *
     * @return a comma delimited list of keyspaces to flush
     */
    public String getFlushKeyspaces();

    /**
     * Interval to be used for flush.
     *
     * @return the interval to run the flush task.  Format is name=value where
     * “name” is an enum of hour, daily, value is ...
     */
    public String getFlushInterval();

    /**
     * Scheduler type to use for flush.
     *
     * @return Type of scheduler to use for flush.  Note the default is TIMER based i.e. to use {@link #getFlushInterval()}.
     * If value of "CRON" is provided it starts using {@link #getFlushCronExpression()}.
     */
    public SchedulerType getFlushSchedulerType() throws UnsupportedTypeException;

    /**
     * Cron expression to be used for flush.
     *
     * @return Cron expression for flush
     * @see <a href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    public String getFlushCronExpression();

    /**
     * @return the absolute path to store the backup status on disk
     */
    public String getBackupStatusFileLoc();

    public boolean useSudo();

    /**
     * SNS Notification topic to be used for sending backup event notifications.
     * One start event is sent before uploading any file and one complete/failure event is sent after the file is uploaded/failed. This applies to both incremental and snapshot.
     * Default: no notifications i.e. this value is set to EMPTY VALUE
     * @return SNS Topic ARN to be used to send notification.
     */
    public String getBackupNotificationTopicArn();
}
