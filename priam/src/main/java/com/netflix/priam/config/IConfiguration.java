/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.config;

import com.google.inject.ImplementedBy;
import com.netflix.priam.tuner.JVMOption;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.tuner.GCType;
import com.netflix.priam.identity.config.InstanceDataRetriever;
import com.netflix.priam.scheduler.SchedulerType;
import com.netflix.priam.scheduler.UnsupportedTypeException;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for Priam's configuration
 */
@ImplementedBy(PriamConfiguration.class)
public interface IConfiguration {

    void intialize();

    /**
     * @return Path to the home dir of Cassandra
     */
    String getCassHome();

    /**
     * Returns the path to the Cassandra configuration directory, this varies by distribution but the default
     * of finding the yaml file and going up one directory is a reasonable default. If you have a custom Yaml
     * Location that doesn't have all the other configuration files you may want to tune this.
     * @return
     */
    default String getCassConfigurationDirectory() {
        return new File(getYamlLocation()).getParentFile().getPath();
    }

    String getYamlLocation();

    /**
     * @return Path to jvm.options file. This is used to pass JVM options to Cassandra.
     */
    String getJVMOptionsFileLocation();

    /**
     * @return Type of garbage collection mechanism to use for Cassandra. Supported values are CMS,G1GC
     */
    GCType getGCType() throws UnsupportedTypeException;

    /**
     * @return Set of JVM options to exclude/comment.
     */
    Map<String, JVMOption> getJVMExcludeSet();

    /**
     * @return Set of JMV options to add/upsert
     */
    Map<String, JVMOption> getJVMUpsertSet();

    /**
     * @return Path to Cassandra startup script
     */
    String getCassStartupScript();

    /**
     * @return Path to Cassandra stop sript
     */
    String getCassStopScript();

    /**
     * @return int representing how many seconds Priam should fail healthchecks for before gracefully draining (nodetool drain)
     * cassandra prior to stop. If this number is negative then no draining occurs and Priam immediately stops Cassanddra
     * using the provided stop script. If this number is &gt;= 0 then Priam will fail healthchecks for this number of
     * seconds before gracefully draining cassandra (nodetool drain) and stopping cassandra with the stop script.
     */
    int getGracefulDrainHealthWaitSeconds();

    /**
     * @return int representing how often (in seconds) Priam should auto-remediate Cassandra process crash
     * If zero, Priam will restart Cassandra whenever it notices it is crashed
     * If a positive number, Priam will restart cassandra no more than once in that number of seconds. For example a
     * value of 60 means that Priam will only restart Cassandra once per 60 seconds
     * If a negative number, Priam will not restart Cassandra due to crash at all
     */
    int getRemediateDeadCassandraRate();

    /**
     * Eg: 'my_backup' will result in all files stored under this dir/prefix
     *
     * @return Prefix that will be added to remote backup location
     */
    String getBackupLocation();

    /**
     * @return Get Backup retention in days
     */
    int getBackupRetentionDays();

    /**
     * @return Get list of racs to backup. Backup all racs if empty
     */
    List<String> getBackupRacs();

    /**
     * Bucket name in case of AWS
     *
     * @return Bucket name used for backups
     */
    String getBackupPrefix();

    /**
     * Location containing backup files. Typically bucket name followed by path
     * to the clusters backup
     */
    String getRestorePrefix();

    /**
     * @param prefix Set the current restore prefix
     */
    void setRestorePrefix(String prefix);

    /**
     * @return List of keyspaces to restore. If none, all keyspaces are
     * restored.
     */
    List<String> getRestoreKeySpaces();

    /**
     * @return Location of the local data dir
     */
    String getDataFileLocation();

    /**
     * Path where cassandra logs should be stored. This is passed to Cassandra as where to store logs.
     * @return Path to cassandra logs.
     */
    String getLogDirLocation();

    /**
     * @return Location of the hints data directory
     */
    String getHintsLocation();
    /**
     * @return Location of local cache
     */
    String getCacheLocation();

    /**
     * @return Location of local commit log dir
     */
    String getCommitLogLocation();

    /**
     * @return Remote commit log location for backups
     */
    String getBackupCommitLogLocation();

    /**
     * @return Preferred data part size for multi part uploads
     */
    long getBackupChunkSize();

    /**
     * @return Cassandra's JMX port
     */
    int getJmxPort();

    /**
     * @return Cassandra's JMX username
     */
    String getJmxUsername();

    /**
     * @return Cassandra's JMX password
     */
    String getJmxPassword();

    /**
     * @return Enables Remote JMX connections n C*
     */
    boolean enableRemoteJMX();


    /**
     * Cassandra storage/cluster communication port
     */
    int getStoragePort();

    int getSSLStoragePort();

    /**
     * @return Port for CQL binary transport.
     */
    int getNativeTransportPort();

    /**
     * @return Snitch to be used in cassandra.yaml
     */
    String getSnitch();

    /**
     * @return Cluster name
     */
    String getAppName();

    /**
     * @return RAC (or zone for AWS)
     */
    String getRac();

    /**
     * @return List of all RAC used for the cluster
     */
    List<String> getRacs();

    /**
     * @return Local hostmame
     */
    String getHostname();

    /**
     * @return Get instance name (for AWS)
     */
    String getInstanceName();

    /**
     * @return Max heap size be used for Cassandra
     */
    String getHeapSize();

    /**
     * @return New heap size for Cassandra
     */
    String getHeapNewSize();

    /**
     * Cron expression to be used to schedule regular compactions. Use "-1" to disable the CRON. Default: -1
     *
     * @return Compaction cron expression.
     * @see <a href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    default String getCompactionCronExpression(){
        return "-1";
    }

   /**
    * Column Family(ies), comma delimited, to start compactions (user-initiated or on CRON).
    * Note 1: The expected format is keyspace.cfname. If no value is provided then compaction is scheduled for all KS,CF(s)
    * Note 2: CF name allows special character "*" to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1.
    * Note 3: {@link #getCompactionExcludeCFList()} is applied first to exclude CF/keyspace and then {@link #getCompactionIncludeCFList()} is applied to include the CF's/keyspaces.
    * @return Column Family(ies), comma delimited, to start compactions.  If no filter is applied, returns null.
    */
    default String getCompactionIncludeCFList(){
        return null;
    }


    /**
     * Column family(ies), comma delimited, to exclude while starting compaction (user-initiated or on CRON).
     * Note 1: The expected format is keyspace.cfname. If no value is provided then compaction is scheduled for all KS,CF(s)
     * Note 2: CF name allows special character "*" to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1.
     * Note 3: {@link #getCompactionExcludeCFList()} is applied first to exclude CF/keyspace and then {@link #getCompactionIncludeCFList()} is applied to include the CF's/keyspaces.
     * @return Column Family(ies), comma delimited, to exclude from compactions.  If no filter is applied, returns null.
     */
    default String getCompactionExcludeCFList(){
        return null;
    }

    /**
     * @return Backup hour for snapshot backups (0 - 23)
     * @deprecated Use the {{@link #getBackupCronExpression()}} instead.
     */
    @Deprecated
    int getBackupHour();

    /**
     * Cron expression to be used for snapshot backups.
     *
     * @return Backup cron expression for snapshots
     * @see <a href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    String getBackupCronExpression();

    /**
     * Backup scheduler type to use for backup.
     *
     * @return Type of scheduler to use for backup.  Note the default is TIMER based i.e. to use {@link #getBackupHour()}.
     * If value of "CRON" is provided it starts using {@link #getBackupCronExpression()}.
     */
    SchedulerType getBackupSchedulerType() throws UnsupportedTypeException;

    /*
     * @return key spaces, comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    String getSnapshotKeyspaceFilters();

    /*
     * Column Family(ies), comma delimited, to filter from backup.
     * *Note:  the expected format is keyspace.cfname  
     * 
     * @return Column Family(ies), comma delimited, to filter from backup.  If no filter is applied, returns null.
     */
    String getSnapshotCFFilter();

    /*
     * @return key spaces, comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    String getIncrementalKeyspaceFilters();

    /*
     * Column Family(ies), comma delimited, to filter from backup.
     * *Note:  the expected format is keyspace.cfname  
     * 
     * @return Column Family(ies), comma delimited, to filter from backup.  If no filter is applied, returns null.
     */
    String getIncrementalCFFilter();

    /*
     * @return key spaces, comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    String getRestoreKeyspaceFilter();

    /*
     * Column Family(ies), comma delimited, to filter from backup.
     * Note:  the expected format is keyspace.cfname  
     * 
     * @return Column Family(ies), comma delimited, to filter from restore.  If no filter is applied, returns null or empty string.
     */
    String getRestoreCFFilter();

    /**
     * Specifies the start and end time used for restoring data (yyyyMMddHHmm
     * format) Eg: 201201132030,201201142030
     *
     * @return Snapshot to be searched and restored
     */
    String getRestoreSnapshot();

    /**
     * @return Get the region to connect to SDB for instance identity
     */
    String getSDBInstanceIdentityRegion();

    /**
     * @return Get the Data Center name (or region for AWS)
     */
    String getDC();

    /**
     * @param region Set the current data center
     */
    void setDC(String region);

    /**
     * @return true if it is a multi regional cluster
     */
    boolean isMultiDC();

    /**
     * @return Number of backup threads for uploading
     */
    int getMaxBackupUploadThreads();

    /**
     * @return Number of download threads
     */
    int getMaxBackupDownloadThreads();

    /**
     * @return true if restore should search for nearest token if current token
     * is not found
     */
    boolean isRestoreClosestToken();

    /**
     * Amazon specific setting to query ASG Membership
     */
    String getASGName();

    /**
     * Amazon specific setting to query Additional/ Sibling ASG Memberships in csv format to consider while calculating RAC membership
     */
    String getSiblingASGNames();

    /**
     * Get the security group associated with nodes in this cluster
     */
    String getACLGroupName();

    /**
     * @return true if incremental backups are enabled
     */
    boolean isIncrBackup();

    /**
     * @return Get host IP
     */
    String getHostIP();

    /**
     * @return Bytes per second to throttle for backups
     */
    int getUploadThrottle();

    /**
     * @return InstanceDataRetriever which encapsulates meta-data about the running instance like region, RAC, name, ip address etc.
     */
    InstanceDataRetriever getInstanceDataRetriever() throws InstantiationException, IllegalAccessException, ClassNotFoundException;

    /**
     * @return true if Priam should local config file for tokens and seeds
     */
    boolean isLocalBootstrapEnabled();

    /**
     * @return In memory compaction limit
     */
    int getInMemoryCompactionLimit();

    /**
     * @return Compaction throughput
     */
    int getCompactionThroughput();

    /**
     * @return compaction_throughput_mb_per_sec
     */
    int getMaxHintWindowInMS();

    /**
     * @return hinted_handoff_throttle_in_kb
     */
    int getHintedHandoffThrottleKb();


    /**
     * @return Size of Cassandra max direct memory
     */
    String getMaxDirectMemory();

    /**
     * @return Bootstrap cluster name (depends on another cass cluster)
     */
    String getBootClusterName();

    /**
     * @return Get the name of seed provider
     */
    String getSeedProviderName();

    /**
     * @return memtable_cleanup_threshold in C* yaml
     */
    double getMemtableCleanupThreshold();

    /**
     * @return stream_throughput_outbound_megabits_per_sec in yaml
     */
    int getStreamingThroughputMB();


    /**
     * Get the paritioner for this cassandra cluster/node.
     *
     * @return the fully-qualified name of the partitioner class
     */
    String getPartitioner();

    /**
     * Support for c* 1.1 global key cache size
     */
    String getKeyCacheSizeInMB();

    /**
     * Support for limiting the total number of keys in c* 1.1 global key cache.
     */
    String getKeyCacheKeysToSave();

    /**
     * Support for c* 1.1 global row cache size
     */
    String getRowCacheSizeInMB();

    /**
     * Support for limiting the total number of rows in c* 1.1 global row cache.
     */
    String getRowCacheKeysToSave();

    /**
     * @return C* Process Name
     */
    String getCassProcessName();

    /**
     * Defaults to 'allow all'.
     */
    String getAuthenticator();

    /**
     * Defaults to 'allow all'.
     */
    String getAuthorizer();

    /**
     * @return true/false, if Cassandra needs to be started manually
     */
    boolean doesCassandraStartManually();

    /**
     * @return possible values: all, dc, none
     */
    String getInternodeCompression();

    /**
     * Enable/disable backup/restore of commit logs.
     * @return boolean value true if commit log backup/restore is enabled, false otherwise. Default: false.
     */
    boolean isBackingUpCommitLogs();

    String getCommitLogBackupPropsFile();

    String getCommitLogBackupArchiveCmd();

    String getCommitLogBackupRestoreCmd();

    String getCommitLogBackupRestoreFromDirs();

    String getCommitLogBackupRestorePointInTime();

    int maxCommitLogsRestore();

    /**
     * @return true/false, if Cassandra is running in a VPC environment
     */
    boolean isVpcRing();

    void setRestoreKeySpaces(List<String> keyspaces);

    boolean isClientSslEnabled();

    String getInternodeEncryption();

    boolean isDynamicSnitchEnabled();

    boolean isNativeTransportEnabled();

    int getConcurrentReadsCnt();

    int getConcurrentWritesCnt();

    int getConcurrentCompactorsCnt();

    int getIndexInterval();

    /*
     * @return the warning threshold in MB's for large partitions encountered during compaction.
     * Default value of 100 is used (default from cassandra.yaml)
     */
    int getCompactionLargePartitionWarnThresholdInMB();

    String getExtraConfigParams();

    String getCassYamlVal(String priamKey);

    boolean getAutoBoostrap();

    //if using with Datastax Enterprise
    String getDseClusterType();

    boolean isCreateNewTokenEnable();

    /*
     * @return the location on disk of the private key used by the cryptography algorithm
     */
    String getPrivateKeyLocation();

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
    String getRestoreSourceType();

    /*
     * @return true to enable encryption of backup (snapshots, incrementals, commit logs).
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality. 
     */
    boolean isEncryptBackupEnabled();

    /**
     * Data that needs to be restored is encrypted?
     * @return true if data that needs to be restored is encrypted. Note that setting this value does not play any role until {@link #getRestoreSnapshot()} is set to a non-null value.
     */
    boolean isRestoreEncrypted();

    /*
     * @return the Amazon Resource Name (ARN).  This is applicable when restoring from an AWS account which requires cross account assumption. 
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality.
     */
    String getAWSRoleAssumptionArn();

    /*
     * @return Google Cloud Storage service account id to be use within the restore functionality.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality.
     */
    String getGcsServiceAccountId();

    /*
     * @return the absolute path on disk for the Google Cloud Storage PFX file (i.e. the combined format of the private key and certificate).  
     * This information is to be use within the restore functionality.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality.
     */
    String getGcsServiceAccountPrivateKeyLoc();

    /*
     * @return the pass phrase use by PGP cryptography.  This information is to be use within the restore and backup functionality when encryption is enabled.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality. 
     */
    String getPgpPasswordPhrase();

    /*
     * @return public key use by PGP cryptography.  This information is to be use within the restore and backup functionality when encryption is enabled.
     * Note: for backward compatibility, this property should be optional.  Specifically, if it does not exist, it should not cause an adverse impact on current functionality. 
     */
    String getPgpPublicKeyLoc();

    /**
     * Use this method for adding extra/ dynamic cassandra startup options or env properties
     *
     * @return
     */
    Map<String, String> getExtraEnvParams();

    /*
     * @return the vpc id of the running instance.
     */
    String getVpcId();

    /*
     * @return the Amazon Resource Name (ARN) for EC2 classic. 
     */
    String getClassicEC2RoleAssumptionArn();

    /*
     * @return the Amazon Resource Name (ARN) for VPC. 
     */
    String getVpcEC2RoleAssumptionArn();

    /*
     * @return if the dual account support
     */
    boolean isDualAccount();

    Boolean isIncrBackupParallelEnabled();

    /*
     * The number of workers for parallel uploads.
     */
    int getIncrementalBkupMaxConsumers();

    /*
     * The max number of files queued to be uploaded.
     */
    int getUncrementalBkupQueueSize();

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
    String getFlushKeyspaces();

    /**
     * Interval to be used for flush.
     *
     * @return the interval to run the flush task.  Format is name=value where
     * “name” is an enum of hour, daily, value is ...
     * @deprecated Use the {{@link #getFlushCronExpression()} instead.
     */
    @Deprecated
    String getFlushInterval();

    /**
     * Scheduler type to use for flush.
     *
     * @return Type of scheduler to use for flush.  Note the default is TIMER based i.e. to use {@link #getFlushInterval()}.
     * If value of "CRON" is provided it starts using {@link #getFlushCronExpression()}.
     */
    SchedulerType getFlushSchedulerType() throws UnsupportedTypeException;

    /**
     * Cron expression to be used for flush. Use "-1" to disable the CRON. Default: -1
     *
     * @return Cron expression for flush
     * @see <a href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    default String getFlushCronExpression(){
        return "-1";
    }

    /**
     * @return the absolute path to store the backup status on disk
     */
    String getBackupStatusFileLoc();

    boolean useSudo();

    /**
     * SNS Notification topic to be used for sending backup event notifications.
     * One start event is sent before uploading any file and one complete/failure event is sent after the file is uploaded/failed. This applies to both incremental and snapshot.
     * Default: no notifications i.e. this value is set to EMPTY VALUE
     * @return SNS Topic ARN to be used to send notification.
     */
    String getBackupNotificationTopicArn();

    /**
     * Post restore hook enabled state. If enabled, jar represented by getPostRepairHook is called once download of files is complete, before starting Cassandra.
     * @return if post restore hook is enabled
     */
    boolean isPostRestoreHookEnabled();

    /**
     * Post restore hook to be executed
     * @return post restore hook to be executed once restore is complete
     */
    String getPostRestoreHook();


    /**
     * HeartBeat file of post restore hook
     * @return file that indicates heartbeat of post restore hook
     */
    String getPostRestoreHookHeartbeatFileName();


    /**
     * Done file for post restore hook
     * @return file that indicates completion of post restore hook
     */
    String getPostRestoreHookDoneFileName();

    /**
     * Maximum time Priam has to wait for post restore hook sub-process to complete successfully
     * @return time out for post restore hook in days
     */
    int getPostRestoreHookTimeOutInDays();

    /**
     * Heartbeat timeout (in ms) for post restore hook
     * @return heartbeat timeout for post restore hook
     */
    default int getPostRestoreHookHeartBeatTimeoutInMs() {
        return 120000;
    }

    /**
     * Heartbeat check frequency (in ms) for post restore hook
     * @return heart beat check frequency for post restore hook
     */
    default int getPostRestoreHookHeartbeatCheckFrequencyInMs() {
        return 120000;
    }

    /**
     * Return a comma delimited list of property files that should be tuned in the configuration
     * directory by Priam. These files live relative to the configuration directory.
     * @return A comma delimited list of relative file paths to the configuration directory
     */
    default String getTunablePropertyFiles() { return ""; }

    /**
     * Escape hatch for getting any arbitrary property by key
     * This is useful so we don't have to keep adding methods to this interface for every single configuration
     * option ever.
     * @param key The arbitrary configuration property to look up
     * @param defaultValue The default value to return if the key is not found.
     * @return The result for the property
     */
    String getProperty(String key, String defaultValue);
}
