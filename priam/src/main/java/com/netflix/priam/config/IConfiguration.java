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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.ImplementedBy;
import com.netflix.priam.scheduler.SchedulerType;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Interface for Priam's configuration */
@ImplementedBy(PriamConfiguration.class)
public interface IConfiguration {

    void initialize();

    /** @return Path to the home dir of Cassandra */
    default String getCassHome() {
        return "/etc/cassandra";
    }

    /** @return Location to `cassandra.yaml`. */
    default String getYamlLocation() {
        return getCassHome() + "/conf/cassandra.yaml";
    }

    /** @return Path to Cassandra startup script */
    default String getCassStartupScript() {
        return "/etc/init.d/cassandra start";
    }

    /** @return Path to Cassandra stop sript */
    default String getCassStopScript() {
        return "/etc/init.d/cassandra stop";
    }

    /**
     * @return int representing how many seconds Priam should fail healthchecks for before
     *     gracefully draining (nodetool drain) cassandra prior to stop. If this number is negative
     *     then no draining occurs and Priam immediately stops Cassanddra using the provided stop
     *     script. If this number is &gt;= 0 then Priam will fail healthchecks for this number of
     *     seconds before gracefully draining cassandra (nodetool drain) and stopping cassandra with
     *     the stop script.
     */
    default int getGracefulDrainHealthWaitSeconds() {
        return -1;
    }

    /**
     * @return int representing how often (in seconds) Priam should auto-remediate Cassandra process
     *     crash If zero, Priam will restart Cassandra whenever it notices it is crashed If a
     *     positive number, Priam will restart cassandra no more than once in that number of
     *     seconds. For example a value of 60 means that Priam will only restart Cassandra once per
     *     60 seconds If a negative number, Priam will not restart Cassandra due to crash at all
     */
    default int getRemediateDeadCassandraRate() {
        return 3600;
    }

    /**
     * Eg: 'my_backup' will result in all files stored under this dir/prefix
     *
     * @return Prefix that will be added to remote backup location
     */
    default String getBackupLocation() {
        return "backup";
    }

    /** @return Get Backup retention in days */
    default int getBackupRetentionDays() {
        return 0;
    }

    /** @return Get list of racs to backup. Backup all racs if empty */
    default List<String> getBackupRacs() {
        return Collections.EMPTY_LIST;
    }

    /**
     * Backup location i.e. remote file system to upload backups. e.g. for S3 it will be s3 bucket
     * name
     *
     * @return Bucket name used for backups
     */
    default String getBackupPrefix() {
        return "cassandra-archive";
    }

    /**
     * @return Location containing backup files. Typically bucket name followed by path to the
     *     clusters backup
     */
    default String getRestorePrefix() {
        return StringUtils.EMPTY;
    }

    /**
     * This is the location of the data/logs/hints for the cassandra. Priam will by default, create
     * all the sub-directories required. This dir should have permission to be altered by both
     * cassandra and Priam. If this is configured correctly, there is no need to configure {@link
     * #getDataFileLocation()}, {@link #getLogDirLocation()}, {@link #getCacheLocation()} and {@link
     * #getCommitLogLocation()}. Alternatively all the other directories should be set explicitly by
     * user. Set this location to a drive with fast read/writes performance and sizable disk space.
     *
     * @return Location where all the data/logs/hints for the cassandra will sit.
     */
    default String getCassandraBaseDirectory() {
        return "/var/lib/cassandra";
    }

    /** @return Location of the local data dir */
    default String getDataFileLocation() {
        return getCassandraBaseDirectory() + "/data";
    }

    default String getLogDirLocation() {
        return getCassandraBaseDirectory() + "/logs";
    }

    /** @return Location of local cache */
    default String getCacheLocation() {
        return getCassandraBaseDirectory() + "/saved_caches";
    }

    /** @return Location of local commit log dir */
    default String getCommitLogLocation() {
        return getCassandraBaseDirectory() + "/commitlog";
    }

    /** @return Remote commit log location for backups */
    default String getBackupCommitLogLocation() {
        return StringUtils.EMPTY;
    }

    /** @return Preferred data part size for multi part uploads */
    default long getBackupChunkSize() {
        return 10 * 1024 * 1024L;
    }

    /** @return Cassandra's JMX port */
    default int getJmxPort() {
        return 7199;
    }

    /** @return Cassandra's JMX username */
    default String getJmxUsername() {
        return null;
    }

    /** @return Cassandra's JMX password */
    default String getJmxPassword() {
        return null;
    }

    /** @return Enables Remote JMX connections n C* */
    default boolean enableRemoteJMX() {
        return false;
    }

    /** @return Cassandra storage/cluster communication port */
    default int getStoragePort() {
        return 7000;
    }

    default int getSSLStoragePort() {
        return 7001;
    }

    /** @return Cassandra's thrift port */
    default int getThriftPort() {
        return 9160;
    }

    /** @return Port for CQL binary transport. */
    default int getNativeTransportPort() {
        return 9042;
    }

    /** @return Snitch to be used in cassandra.yaml */
    default String getSnitch() {
        return "org.apache.cassandra.locator.Ec2Snitch";
    }

    /** @return Cluster name */
    default String getAppName() {
        return "cass_cluster";
    }

    /** @return List of all RAC used for the cluster */
    List<String> getRacs();

    /** @return Max heap size be used for Cassandra */
    default String getHeapSize() {
        return "8G";
    }

    /** @return New heap size for Cassandra */
    default String getHeapNewSize() {
        return "2G";
    }

    /**
     * Cron expression to be used to schedule regular compactions. Use "-1" to disable the CRON.
     * Default: -1
     *
     * @return Compaction cron expression.
     * @see <a
     *     href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    default String getCompactionCronExpression() {
        return "-1";
    }

    /**
     * Column Family(ies), comma delimited, to start compactions (user-initiated or on CRON). Note
     * 1: The expected format is keyspace.cfname. If no value is provided then compaction is
     * scheduled for all KS,CF(s) Note 2: CF name allows special character "*" to denote all the
     * columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Note
     * 3: {@link #getCompactionExcludeCFList()} is applied first to exclude CF/keyspace and then
     * {@link #getCompactionIncludeCFList()} is applied to include the CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to start compactions. If no filter is applied,
     *     returns null.
     */
    default String getCompactionIncludeCFList() {
        return null;
    }

    /**
     * Column family(ies), comma delimited, to exclude while starting compaction (user-initiated or
     * on CRON). Note 1: The expected format is keyspace.cfname. If no value is provided then
     * compaction is scheduled for all KS,CF(s) Note 2: CF name allows special character "*" to
     * denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in
     * keyspace1. Note 3: {@link #getCompactionExcludeCFList()} is applied first to exclude
     * CF/keyspace and then {@link #getCompactionIncludeCFList()} is applied to include the
     * CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to exclude from compactions. If no filter is
     *     applied, returns null.
     */
    default String getCompactionExcludeCFList() {
        return null;
    }

    /**
     * @return Backup hour for snapshot backups (0 - 23)
     * @deprecated Use the {{@link #getBackupCronExpression()}} instead. Scheduled for deletion in
     *     Dec 2018.
     */
    @Deprecated
    default int getBackupHour() {
        return 12;
    }

    /**
     * Cron expression to be used for snapshot backups.
     *
     * @return Backup cron expression for snapshots
     * @see <a
     *     href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    default String getBackupCronExpression() {
        return "0 0 12 1/1 * ? *";
    }

    /**
     * Backup scheduler type to use for backup.
     *
     * @return Type of scheduler to use for backup. Note the default is TIMER based i.e. to use
     *     {@link #getBackupHour()}. If value of "CRON" is provided it starts using {@link
     *     #getBackupCronExpression()}.
     * @throws UnsupportedTypeException if the scheduler type is not CRON/HOUR.
     */
    default SchedulerType getBackupSchedulerType() throws UnsupportedTypeException {
        return SchedulerType.HOUR;
    }

    /**
     * Column Family(ies), comma delimited, to include during snapshot backup. Note 1: The expected
     * format is keyspace.cfname. If no value is provided then snapshot contains all KS,CF(s) Note
     * 2: CF name allows special character "*" to denote all the columnfamilies in a given keyspace.
     * e.g. keyspace1.* denotes all the CFs in keyspace1. Note 3: {@link
     * #getSnapshotExcludeCFList()} is applied first to exclude CF/keyspace and then {@link
     * #getSnapshotIncludeCFList()} is applied to include the CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to include in snapshot backup. If no filter is
     *     applied, returns null.
     */
    default String getSnapshotIncludeCFList() {
        return null;
    }

    /**
     * Column family(ies), comma delimited, to exclude during snapshot backup. Note 1: The expected
     * format is keyspace.cfname. If no value is provided then snapshot is scheduled for all
     * KS,CF(s) Note 2: CF name allows special character "*" to denote all the columnfamilies in a
     * given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Note 3: {@link
     * #getSnapshotExcludeCFList()} is applied first to exclude CF/keyspace and then {@link
     * #getSnapshotIncludeCFList()} is applied to include the CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to exclude from snapshot backup. If no filter is
     *     applied, returns null.
     */
    default String getSnapshotExcludeCFList() {
        return null;
    }

    /**
     * Column Family(ies), comma delimited, to include during incremental backup. Note 1: The
     * expected format is keyspace.cfname. If no value is provided then incremental contains all
     * KS,CF(s) Note 2: CF name allows special character "*" to denote all the columnfamilies in a
     * given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Note 3: {@link
     * #getIncrementalExcludeCFList()} is applied first to exclude CF/keyspace and then {@link
     * #getIncrementalIncludeCFList()} is applied to include the CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to include in incremental backup. If no filter
     *     is applied, returns null.
     */
    default String getIncrementalIncludeCFList() {
        return null;
    }

    /**
     * Column family(ies), comma delimited, to exclude during incremental backup. Note 1: The
     * expected format is keyspace.cfname. If no value is provided then incremental is scheduled for
     * all KS,CF(s) Note 2: CF name allows special character "*" to denote all the columnfamilies in
     * a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Note 3: {@link
     * #getIncrementalExcludeCFList()} is applied first to exclude CF/keyspace and then {@link
     * #getIncrementalIncludeCFList()} is applied to include the CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to exclude from incremental backup. If no filter
     *     is applied, returns null.
     */
    default String getIncrementalExcludeCFList() {
        return null;
    }

    /**
     * Column Family(ies), comma delimited, to include during restore. Note 1: The expected format
     * is keyspace.cfname. If no value is provided then restore contains all KS,CF(s) Note 2: CF
     * name allows special character "*" to denote all the columnfamilies in a given keyspace. e.g.
     * keyspace1.* denotes all the CFs in keyspace1. Note 3: {@link #getRestoreExcludeCFList()} is
     * applied first to exclude CF/keyspace and then {@link #getRestoreIncludeCFList()} is applied
     * to include the CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to include in restore. If no filter is applied,
     *     returns null.
     */
    default String getRestoreIncludeCFList() {
        return null;
    }

    /**
     * Column family(ies), comma delimited, to exclude during restore. Note 1: The expected format
     * is keyspace.cfname. If no value is provided then restore is scheduled for all KS,CF(s) Note
     * 2: CF name allows special character "*" to denote all the columnfamilies in a given keyspace.
     * e.g. keyspace1.* denotes all the CFs in keyspace1. Note 3: {@link #getRestoreExcludeCFList()}
     * is applied first to exclude CF/keyspace and then {@link #getRestoreIncludeCFList()} is
     * applied to include the CF's/keyspaces.
     *
     * @return Column Family(ies), comma delimited, to exclude from restore. If no filter is
     *     applied, returns null.
     */
    default String getRestoreExcludeCFList() {
        return null;
    }

    /**
     * Specifies the start and end time used for restoring data (yyyyMMddHHmm format) Eg:
     * 201201132030,201201142030
     *
     * @return Snapshot to be searched and restored
     */
    default String getRestoreSnapshot() {
        return StringUtils.EMPTY;
    }

    /** @return Get the region to connect to SDB for instance identity */
    default String getSDBInstanceIdentityRegion() {
        return "us-east-1";
    }

    /** @return true if it is a multi regional cluster */
    default boolean isMultiDC() {
        return false;
    }

    /** @return Number of backup threads for uploading files when using async feature */
    default int getBackupThreads() {
        return 2;
    }

    /** @return Number of download threads for downloading files when using async feature */
    default int getRestoreThreads() {
        return 8;
    }

    /** @return true if restore should search for nearest token if current token is not found */
    default boolean isRestoreClosestToken() {
        return false;
    }

    /**
     * Amazon specific setting to query Additional/ Sibling ASG Memberships in csv format to
     * consider while calculating RAC membership
     */
    default String getSiblingASGNames() {
        return ",";
    }

    /** Get the security group associated with nodes in this cluster */
    default String getACLGroupName() {
        return getAppName();
    }

    /** @return true if incremental backups are enabled */
    default boolean isIncrementalBackupEnabled() {
        return true;
    }

    /** @return Bytes per second to throttle for backups */
    default int getUploadThrottle() {
        return -1;
    }

    /** @return true if Priam should local config file for tokens and seeds */
    default boolean isLocalBootstrapEnabled() {
        return false;
    }

    /** @return Compaction throughput */
    default int getCompactionThroughput() {
        return 8;
    }

    /** @return compaction_throughput_mb_per_sec */
    default int getMaxHintWindowInMS() {
        return 10800000;
    }

    /** @return hinted_handoff_throttle_in_kb */
    default int getHintedHandoffThrottleKb() {
        return 1024;
    }

    /** @return Size of Cassandra max direct memory */
    default String getMaxDirectMemory() {
        return "50G";
    }

    /** @return Bootstrap cluster name (depends on another cass cluster) */
    default String getBootClusterName() {
        return StringUtils.EMPTY;
    }

    /** @return Get the name of seed provider */
    default String getSeedProviderName() {
        return "com.netflix.priam.cassandra.extensions.NFSeedProvider";
    }

    /**
     * memtable_cleanup_threshold defaults to 1 / (memtable_flush_writers + 1) = 0.11
     *
     * @return memtable_cleanup_threshold in C* yaml
     */
    default double getMemtableCleanupThreshold() {
        return 0.11;
    }

    /** @return stream_throughput_outbound_megabits_per_sec in yaml */
    default int getStreamingThroughputMB() {
        return 400;
    }

    /**
     * Get the paritioner for this cassandra cluster/node.
     *
     * @return the fully-qualified name of the partitioner class
     */
    default String getPartitioner() {
        return "org.apache.cassandra.dht.RandomPartitioner";
    }

    /** Support for c* 1.1 global key cache size */
    default String getKeyCacheSizeInMB() {
        return StringUtils.EMPTY;
    }

    /** Support for limiting the total number of keys in c* 1.1 global key cache. */
    default String getKeyCacheKeysToSave() {
        return StringUtils.EMPTY;
    }

    /** Support for c* 1.1 global row cache size */
    default String getRowCacheSizeInMB() {
        return StringUtils.EMPTY;
    }

    /** Support for limiting the total number of rows in c* 1.1 global row cache. */
    default String getRowCacheKeysToSave() {
        return StringUtils.EMPTY;
    }

    /** @return C* Process Name */
    default String getCassProcessName() {
        return "CassandraDaemon";
    }

    /** Defaults to 'allow all'. */
    default String getAuthenticator() {
        return "org.apache.cassandra.auth.AllowAllAuthenticator";
    }

    /** Defaults to 'allow all'. */
    default String getAuthorizer() {
        return "org.apache.cassandra.auth.AllowAllAuthorizer";
    }

    /** @return true/false, if Cassandra needs to be started manually */
    default boolean doesCassandraStartManually() {
        return false;
    }

    /** @return possible values: all, dc, none */
    default String getInternodeCompression() {
        return "all";
    }

    /**
     * Enable/disable backup/restore of commit logs.
     *
     * @return boolean value true if commit log backup/restore is enabled, false otherwise. Default:
     *     false.
     */
    default boolean isBackingUpCommitLogs() {
        return false;
    }

    default String getCommitLogBackupPropsFile() {
        return getCassHome() + "/conf/commitlog_archiving.properties";
    }

    default String getCommitLogBackupArchiveCmd() {
        return "/bin/ln %path /mnt/data/backup/%name";
    }

    default String getCommitLogBackupRestoreCmd() {
        return "/bin/mv %from %to";
    }

    default String getCommitLogBackupRestoreFromDirs() {
        return "/mnt/data/backup/commitlog/";
    }

    default String getCommitLogBackupRestorePointInTime() {
        return StringUtils.EMPTY;
    }

    default int maxCommitLogsRestore() {
        return 10;
    }

    default boolean isClientSslEnabled() {
        return false;
    }

    default String getInternodeEncryption() {
        return "none";
    }

    default boolean isDynamicSnitchEnabled() {
        return true;
    }

    default boolean isThriftEnabled() {
        return true;
    }

    default boolean isNativeTransportEnabled() {
        return true;
    }

    default int getConcurrentReadsCnt() {
        return 32;
    }

    default int getConcurrentWritesCnt() {
        return 32;
    }

    default int getConcurrentCompactorsCnt() {
        return Runtime.getRuntime().availableProcessors();
    }

    default String getRpcServerType() {
        return "hsha";
    }

    default int getRpcMinThreads() {
        return 16;
    }

    default int getRpcMaxThreads() {
        return 2048;
    }

    /*
     * @return the warning threshold in MB's for large partitions encountered during compaction.
     * Default value of 100 is used (default from cassandra.yaml)
     */
    default int getCompactionLargePartitionWarnThresholdInMB() {
        return 100;
    }

    default String getExtraConfigParams() {
        return StringUtils.EMPTY;
    }

    String getCassYamlVal(String priamKey);

    default boolean getAutoBoostrap() {
        return true;
    }

    default boolean isCreateNewTokenEnable() {
        return true;
    }

    /*
     * @return the location on disk of the private key used by the cryptography algorithm
     */
    default String getPrivateKeyLocation() {
        return StringUtils.EMPTY;
    }

    /**
     * @return the type of source for the restore. Valid values are: AWSCROSSACCT or GOOGLE. Note:
     *     for backward compatibility, this property should be optional. Specifically, if it does
     *     not exist, it should not cause an adverse impact on current functionality.
     *     <p>AWSCROSSACCT - You are restoring from an AWS account which requires cross account
     *     assumption where an IAM user in one account is allowed to access resources that belong to
     *     a different account.
     *     <p>GOOGLE - You are restoring from Google Cloud Storage
     */
    default String getRestoreSourceType() {
        return StringUtils.EMPTY;
    }

    /**
     * Should backups be encrypted. If this is on, then all the files uploaded will be compressed
     * and encrypted before being uploaded to remote file system.
     *
     * @return true to enable encryption of backup (snapshots, incrementals, commit logs). Note: for
     *     backward compatibility, this property should be optional. Specifically, if it does not
     *     exist, it should not cause an adverse impact on current functionality.
     */
    default boolean isEncryptBackupEnabled() {
        return false;
    }

    /**
     * Data that needs to be restored is encrypted?
     *
     * @return true if data that needs to be restored is encrypted. Note that setting this value
     *     does not play any role until {@link #getRestoreSnapshot()} is set to a non-null value.
     */
    default boolean isRestoreEncrypted() {
        return false;
    }

    /**
     * @return the Amazon Resource Name (ARN). This is applicable when restoring from an AWS account
     *     which requires cross account assumption. Note: for backward compatibility, this property
     *     should be optional. Specifically, if it does not exist, it should not cause an adverse
     *     impact on current functionality.
     */
    default String getAWSRoleAssumptionArn() {
        return StringUtils.EMPTY;
    }

    /**
     * @return Google Cloud Storage service account id to be use within the restore functionality.
     *     Note: for backward compatibility, this property should be optional. Specifically, if it
     *     does not exist, it should not cause an adverse impact on current functionality.
     */
    default String getGcsServiceAccountId() {
        return StringUtils.EMPTY;
    }

    /**
     * @return the absolute path on disk for the Google Cloud Storage PFX file (i.e. the combined
     *     format of the private key and certificate). This information is to be use within the
     *     restore functionality. Note: for backward compatibility, this property should be
     *     optional. Specifically, if it does not exist, it should not cause an adverse impact on
     *     current functionality.
     */
    default String getGcsServiceAccountPrivateKeyLoc() {
        return StringUtils.EMPTY;
    }

    /**
     * @return the pass phrase use by PGP cryptography. This information is to be use within the
     *     restore and backup functionality when encryption is enabled. Note: for backward
     *     compatibility, this property should be optional. Specifically, if it does not exist, it
     *     should not cause an adverse impact on current functionality.
     */
    default String getPgpPasswordPhrase() {
        return StringUtils.EMPTY;
    }

    /**
     * @return public key use by PGP cryptography. This information is to be use within the restore
     *     and backup functionality when encryption is enabled. Note: for backward compatibility,
     *     this property should be optional. Specifically, if it does not exist, it should not cause
     *     an adverse impact on current functionality.
     */
    default String getPgpPublicKeyLoc() {
        return StringUtils.EMPTY;
    }

    /**
     * Use this method for adding extra/ dynamic cassandra startup options or env properties
     *
     * @return A map of extra paramaters.
     */
    default Map<String, String> getExtraEnvParams() {
        return Collections.EMPTY_MAP;
    }

    /*
     * @return the Amazon Resource Name (ARN) for EC2 classic.
     */
    default String getClassicEC2RoleAssumptionArn() {
        return StringUtils.EMPTY;
    }

    /*
     * @return the Amazon Resource Name (ARN) for VPC.
     */
    default String getVpcEC2RoleAssumptionArn() {
        return StringUtils.EMPTY;
    }

    /**
     * Is cassandra cluster spanning more than one account. This may be true if you are migrating
     * your cluster from one account to another.
     *
     * @return if the dual account support
     */
    default boolean isDualAccount() {
        return false;
    }

    /**
     * Should incremental backup be uploaded in async fashion? If this is false, then incrementals
     * will be in sync fashion.
     *
     * @return enable async incrementals for backup
     */
    default boolean enableAsyncIncremental() {
        return false;
    }

    /**
     * Should snapshot backup be uploaded in async fashion? If this is false, then snapshot will be
     * in sync fashion.
     *
     * @return enable async snapshot for backup
     */
    default boolean enableAsyncSnapshot() {
        return false;
    }

    /**
     * Queue size to be used for backup uploads. Note that once queue is full, we would wait for
     * {@link #getUploadTimeout()} to add any new item before declining the request and throwing
     * exception.
     *
     * @return size of the queue for uploads.
     */
    default int getBackupQueueSize() {
        return 100000;
    }

    /**
     * Queue size to be used for file downloads. Note that once queue is full, we would wait for
     * {@link #getDownloadTimeout()} to add any new item before declining the request and throwing
     * exception.
     *
     * @return size of the queue for downloads.
     */
    default int getDownloadQueueSize() {
        return 100000;
    }

    /**
     * Uploads are scheduled in {@link #getBackupQueueSize()}. If queue is full then we wait for
     * {@link #getUploadTimeout()} for the queue to have an entry available for queueing the current
     * task after which we throw RejectedExecutionException.
     *
     * @return timeout for uploads to wait to blocking queue
     */
    default long getUploadTimeout() {
        return (2 * 60 * 60 * 1000L); // 2 minutes.
    }

    /**
     * Downloads are scheduled in {@link #getDownloadQueueSize()}. If queue is full then we wait for
     * {@link #getDownloadTimeout()} for the queue to have an entry available for queueing the
     * current task after which we throw RejectedExecutionException.
     *
     * @return timeout for downloads to wait to blocking queue
     */
    default long getDownloadTimeout() {
        return (10 * 60 * 60 * 1000L); // 10 minutes.
    }

    /** @return tombstone_warn_threshold in C* yaml */
    default int getTombstoneWarnThreshold() {
        return 1000;
    }

    /** @return tombstone_failure_threshold in C* yaml */
    default int getTombstoneFailureThreshold() {
        return 100000;
    }

    /** @return streaming_socket_timeout_in_ms in C* yaml */
    default int getStreamingSocketTimeoutInMS() {
        return 86400000;
    }

    /**
     * List of keyspaces to flush. Default: all keyspaces.
     *
     * @return a comma delimited list of keyspaces to flush
     */
    default String getFlushKeyspaces() {
        return StringUtils.EMPTY;
    }

    /**
     * Interval to be used for flush.
     *
     * @return the interval to run the flush task. Format is name=value where “name” is an enum of
     *     hour, daily, value is ...
     * @deprecated Use the {{@link #getFlushCronExpression()} instead. This is set for deletion in
     *     Dec 2018.
     */
    @Deprecated
    default String getFlushInterval() {
        return null;
    }

    /**
     * Scheduler type to use for flush. Default: HOUR.
     *
     * @return Type of scheduler to use for flush. Note the default is TIMER based i.e. to use
     *     {@link #getFlushInterval()}. If value of "CRON" is provided it starts using {@link
     *     #getFlushCronExpression()}.
     * @throws UnsupportedTypeException if the scheduler type is not HOUR/CRON.
     */
    default SchedulerType getFlushSchedulerType() throws UnsupportedTypeException {
        return SchedulerType.HOUR;
    }

    /**
     * Cron expression to be used for flush. Use "-1" to disable the CRON. Default: -1
     *
     * @return Cron expression for flush
     * @see <a
     *     href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    default String getFlushCronExpression() {
        return "-1";
    }

    /** @return the absolute path to store the backup status on disk */
    default String getBackupStatusFileLoc() {
        return getDataFileLocation() + File.separator + "backup.status";
    }

    /** @return Decides whether to use sudo to start C* or not */
    default boolean useSudo() {
        return true;
    }

    /**
     * SNS Notification topic to be used for sending backup event notifications. One start event is
     * sent before uploading any file and one complete/failure event is sent after the file is
     * uploaded/failed. This applies to both incremental and snapshot. Default: no notifications
     * i.e. this value is set to EMPTY VALUE
     *
     * @return SNS Topic ARN to be used to send notification.
     */
    default String getBackupNotificationTopicArn() {
        return StringUtils.EMPTY;
    }

    /**
     * Post restore hook enabled state. If enabled, jar represented by getPostRepairHook is called
     * once download of files is complete, before starting Cassandra.
     *
     * @return if post restore hook is enabled
     */
    default boolean isPostRestoreHookEnabled() {
        return false;
    }

    /**
     * Post restore hook to be executed
     *
     * @return post restore hook to be executed once restore is complete
     */
    default String getPostRestoreHook() {
        return StringUtils.EMPTY;
    }

    /**
     * HeartBeat file of post restore hook
     *
     * @return file that indicates heartbeat of post restore hook
     */
    default String getPostRestoreHookHeartbeatFileName() {
        return "postrestorehook_heartbeat";
    }

    /**
     * Done file for post restore hook
     *
     * @return file that indicates completion of post restore hook
     */
    default String getPostRestoreHookDoneFileName() {
        return "postrestorehook_done";
    }

    /**
     * Maximum time Priam has to wait for post restore hook sub-process to complete successfully
     *
     * @return time out for post restore hook in days
     */
    default int getPostRestoreHookTimeOutInDays() {
        return 2;
    }

    /**
     * Heartbeat timeout (in ms) for post restore hook
     *
     * @return heartbeat timeout for post restore hook
     */
    default int getPostRestoreHookHeartBeatTimeoutInMs() {
        return 120000;
    }

    /**
     * Heartbeat check frequency (in ms) for post restore hook
     *
     * @return heart beat check frequency for post restore hook
     */
    default int getPostRestoreHookHeartbeatCheckFrequencyInMs() {
        return 120000;
    }

    /**
     * Grace period for the file(that should have been deleted by cassandra) that are considered to
     * be forgotten. Only required for cassandra 2.x.
     *
     * @return grace period for the forgotten files.
     */
    default int getForgottenFileGracePeriodDays() {
        return 1;
    }

    /**
     * If any forgotten file is found in Cassandra, it is usually good practice to move/delete them
     * so when cassandra restarts, it does not load old data which should be removed else you may
     * run into data resurrection issues. This behavior is fixed in 3.x. This configuration will
     * allow Priam to move the forgotten files to a "lost_found" directory for user to review at
     * later time at the same time ensuring that Cassandra does not resurrect data.
     *
     * @return true if Priam should move forgotten file to "lost_found" directory of that CF.
     */
    default boolean isForgottenFileMoveEnabled() {
        return false;
    }

    /**
     * A method for allowing access to outside programs to Priam configuration when paired with the
     * Priam configuration HTTP endpoint at /v1/config/structured/all/property
     *
     * @param group The group of configuration options to return, currently just returns everything
     *     no matter what
     * @return A Map representation of this configuration, or null if the method doesn't exist
     */
    @SuppressWarnings("unchecked")
    @JsonIgnore
    default Map<String, Object> getStructuredConfiguration(String group) {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.convertValue(this, Map.class);
    }

    /**
     * Cron expression to be used for persisting Priam merged configuration to disk. Use "-1" to
     * disable the CRON. This will persist the fully merged value of Priam's configuration to the
     * {@link #getMergedConfigurationDirectory()} as two JSON files: structured.json and
     * unstructured.json which persist structured config and unstructured config respectively. We
     * recommend you only rely on unstructured for the time being until the structured interface is
     * finalized.
     *
     * <p>Default: every minute
     *
     * @return Cron expression for merged configuration writing
     * @see <a
     *     href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    default String getMergedConfigurationCronExpression() {
        // Every minute on the top of the minute.
        return "0 * * * * ? *";
    }

    /**
     * Returns the path to the directory that Priam should write merged configuration to. Note that
     * if you disable the merged configuration cron above {@link
     * #getMergedConfigurationCronExpression()} then this directory is not created or used
     *
     * @return A string representation of the path to the merged priam configuration directory.
     */
    default String getMergedConfigurationDirectory() {
        return "/tmp/priam_configuration";
    }

    /**
     * Escape hatch for getting any arbitrary property by key This is useful so we don't have to
     * keep adding methods to this interface for every single configuration option ever. Also
     * exposed via HTTP at v1/config/unstructured/X
     *
     * @param key The arbitrary configuration property to look up
     * @param defaultValue The default value to return if the key is not found.
     * @return The result for the property, or the defaultValue if provided (null otherwise)
     */
    String getProperty(String key, String defaultValue);
}
