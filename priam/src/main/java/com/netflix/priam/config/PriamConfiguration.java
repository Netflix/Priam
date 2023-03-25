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
import com.netflix.priam.configSource.IConfigSource;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.tuner.GCType;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PriamConfiguration implements IConfiguration {
    public static final String PRIAM_PRE = "priam";

    private final IConfigSource config;
    private static final Logger logger = LoggerFactory.getLogger(PriamConfiguration.class);

    @JsonIgnore private InstanceInfo instanceInfo;

    @Inject
    public PriamConfiguration(IConfigSource config, InstanceInfo instanceInfo) {
        this.config = config;
        this.instanceInfo = instanceInfo;
    }

    @Override
    public void initialize() {
        this.config.initialize(instanceInfo.getAutoScalingGroup(), instanceInfo.getRegion());
    }

    @Override
    public String getCassStartupScript() {
        return config.get(PRIAM_PRE + ".cass.startscript", "/etc/init.d/cassandra start");
    }

    @Override
    public String getCassStopScript() {
        return config.get(PRIAM_PRE + ".cass.stopscript", "/etc/init.d/cassandra stop");
    }

    @Override
    public int getGracefulDrainHealthWaitSeconds() {
        return -1;
    }

    @Override
    public int getRemediateDeadCassandraRate() {
        return config.get(
                PRIAM_PRE + ".remediate.dead.cassandra.rate", 3600); // Default to once per hour
    }

    @Override
    public String getCassHome() {
        return config.get(PRIAM_PRE + ".cass.home", "/etc/cassandra");
    }

    @Override
    public String getBackupLocation() {
        return config.get(PRIAM_PRE + ".s3.base_dir", "backup");
    }

    @Override
    public String getBackupPrefix() {
        return config.get(PRIAM_PRE + ".s3.bucket", "cassandra-archive");
    }

    @Override
    public int getBackupRetentionDays() {
        return config.get(PRIAM_PRE + ".backup.retention", 0);
    }

    @Override
    public List<String> getBackupRacs() {
        return config.getList(PRIAM_PRE + ".backup.racs");
    }

    @Override
    public String getRestorePrefix() {
        return config.get(PRIAM_PRE + ".restore.prefix");
    }

    @Override
    public String getDataFileLocation() {
        return config.get(PRIAM_PRE + ".data.location", getCassandraBaseDirectory() + "/data");
    }

    @Override
    public String getLogDirLocation() {
        return config.get(PRIAM_PRE + ".logs.location", getCassandraBaseDirectory() + "/logs");
    }

    @Override
    public String getCacheLocation() {
        return config.get(
                PRIAM_PRE + ".cache.location", getCassandraBaseDirectory() + "/saved_caches");
    }

    @Override
    public String getCommitLogLocation() {
        return config.get(
                PRIAM_PRE + ".commitlog.location", getCassandraBaseDirectory() + "/commitlog");
    }

    @Override
    public String getBackupCommitLogLocation() {
        return config.get(PRIAM_PRE + ".backup.commitlog.location", "");
    }

    @Override
    public long getBackupChunkSize() {
        long size = config.get(PRIAM_PRE + ".backup.chunksizemb", 10);
        return size * 1024 * 1024L;
    }

    @Override
    public int getJmxPort() {
        return config.get(PRIAM_PRE + ".jmx.port", 7199);
    }

    @Override
    public String getJmxUsername() {
        return config.get(PRIAM_PRE + ".jmx.username", "");
    }

    @Override
    public String getJmxPassword() {
        return config.get(PRIAM_PRE + ".jmx.password", "");
    }

    /** @return Enables Remote JMX connections n C* */
    @Override
    public boolean enableRemoteJMX() {
        return config.get(PRIAM_PRE + ".jmx.remote.enable", false);
    }

    public int getNativeTransportPort() {
        return config.get(PRIAM_PRE + ".nativeTransport.port", 9042);
    }

    @Override
    public int getThriftPort() {
        return config.get(PRIAM_PRE + ".thrift.port", 9160);
    }

    @Override
    public int getStoragePort() {
        return config.get(PRIAM_PRE + ".storage.port", 7000);
    }

    @Override
    public int getSSLStoragePort() {
        return config.get(PRIAM_PRE + ".ssl.storage.port", 7001);
    }

    @Override
    public String getSnitch() {
        return config.get(PRIAM_PRE + ".endpoint_snitch", "org.apache.cassandra.locator.Ec2Snitch");
    }

    @Override
    public String getAppName() {
        return config.get(PRIAM_PRE + ".clustername", "cass_cluster");
    }

    @Override
    public List<String> getRacs() {
        return config.getList(PRIAM_PRE + ".zones.available", instanceInfo.getDefaultRacks());
    }

    @Override
    public String getHeapSize() {
        return config.get((PRIAM_PRE + ".heap.size.") + instanceInfo.getInstanceType(), "8G");
    }

    @Override
    public String getHeapNewSize() {
        return config.get(
                (PRIAM_PRE + ".heap.newgen.size.") + instanceInfo.getInstanceType(), "2G");
    }

    @Override
    public String getMaxDirectMemory() {
        return config.get(
                (PRIAM_PRE + ".direct.memory.size.") + instanceInfo.getInstanceType(), "50G");
    }

    @Override
    public String getBackupCronExpression() {
        return config.get(PRIAM_PRE + ".backup.cron", "0 0 12 1/1 * ? *"); // Backup daily at 12
    }

    @Override
    public GCType getGCType() throws UnsupportedTypeException {
        String gcType = config.get(PRIAM_PRE + ".gc.type", GCType.CMS.getGcType());
        return GCType.lookup(gcType);
    }

    @Override
    public String getJVMExcludeSet() {
        return config.get(PRIAM_PRE + ".jvm.options.exclude");
    }

    @Override
    public String getJVMUpsertSet() {
        return config.get(PRIAM_PRE + ".jvm.options.upsert");
    }

    @Override
    public String getFlushCronExpression() {
        return config.get(PRIAM_PRE + ".flush.cron", "-1");
    }

    @Override
    public String getCompactionCronExpression() {
        return config.get(PRIAM_PRE + ".compaction.cron", "-1");
    }

    @Override
    public String getCompactionIncludeCFList() {
        return config.get(PRIAM_PRE + ".compaction.cf.include");
    }

    @Override
    public String getCompactionExcludeCFList() {
        return config.get(PRIAM_PRE + ".compaction.cf.exclude");
    }

    @Override
    public String getSnapshotIncludeCFList() {
        return config.get(PRIAM_PRE + ".snapshot.cf.include");
    }

    @Override
    public String getSnapshotExcludeCFList() {
        return config.get(PRIAM_PRE + ".snapshot.cf.exclude");
    }

    @Override
    public String getIncrementalIncludeCFList() {
        return config.get(PRIAM_PRE + ".incremental.cf.include");
    }

    @Override
    public String getIncrementalExcludeCFList() {
        return config.get(PRIAM_PRE + ".incremental.cf.exclude");
    }

    @Override
    public String getRestoreIncludeCFList() {
        return config.get(PRIAM_PRE + ".restore.cf.include");
    }

    @Override
    public String getRestoreExcludeCFList() {
        return config.get(PRIAM_PRE + ".restore.cf.exclude");
    }

    @Override
    public String getRestoreSnapshot() {
        return config.get(PRIAM_PRE + ".restore.snapshot", "");
    }

    @Override
    public boolean isRestoreEncrypted() {
        return config.get(PRIAM_PRE + ".encrypted.restore.enabled", false);
    }

    @Override
    public String getSDBInstanceIdentityRegion() {
        return config.get(PRIAM_PRE + ".sdb.instanceIdentity.region", "us-east-1");
    }

    @Override
    public boolean isMultiDC() {
        return config.get(PRIAM_PRE + ".multiregion.enable", false);
    }

    @Override
    public int getBackupThreads() {
        return config.get(PRIAM_PRE + ".backup.threads", 2);
    }

    @Override
    public int getRestoreThreads() {
        return config.get(PRIAM_PRE + ".restore.threads", 8);
    }

    @Override
    public boolean isRestoreClosestToken() {
        return config.get(PRIAM_PRE + ".restore.closesttoken", false);
    }

    @Override
    public String getSiblingASGNames() {
        return config.get(PRIAM_PRE + ".az.sibling.asgnames", ",");
    }

    @Override
    public String getACLGroupName() {
        return config.get(PRIAM_PRE + ".acl.groupname", this.getAppName());
    }

    @Override
    public boolean isIncrementalBackupEnabled() {
        return config.get(PRIAM_PRE + ".backup.incremental.enable", true);
    }

    @Override
    public int getUploadThrottle() {
        return config.get(PRIAM_PRE + ".upload.throttle", -1);
    }

    @Override
    public int getRemoteFileSystemObjectExistsThrottle() {
        return config.get(PRIAM_PRE + ".remoteFileSystemObjectExistThrottle", -1);
    }

    @Override
    public boolean isLocalBootstrapEnabled() {
        return config.get(PRIAM_PRE + ".localbootstrap.enable", false);
    }

    @Override
    public int getCompactionThroughput() {
        return config.get(PRIAM_PRE + ".compaction.throughput", 8);
    }

    @Override
    public int getMaxHintWindowInMS() {
        return config.get(PRIAM_PRE + ".hint.window", 10800000);
    }

    public int getHintedHandoffThrottleKb() {
        return config.get(PRIAM_PRE + ".hints.throttleKb", 1024);
    }

    @Override
    public String getBootClusterName() {
        return config.get(PRIAM_PRE + ".bootcluster", "");
    }

    @Override
    public String getSeedProviderName() {
        return config.get(
                PRIAM_PRE + ".seed.provider",
                "com.netflix.priam.cassandra.extensions.NFSeedProvider");
    }

    public double getMemtableCleanupThreshold() {
        return config.get(PRIAM_PRE + ".memtable.cleanup.threshold", 0.11);
    }

    @Override
    public int getStreamingThroughputMB() {
        return config.get(PRIAM_PRE + ".streaming.throughput.mb", 400);
    }

    public String getPartitioner() {
        return config.get(PRIAM_PRE + ".partitioner", "org.apache.cassandra.dht.RandomPartitioner");
    }

    public String getKeyCacheSizeInMB() {
        return config.get(PRIAM_PRE + ".keyCache.size");
    }

    public String getKeyCacheKeysToSave() {
        return config.get(PRIAM_PRE + ".keyCache.count");
    }

    public String getRowCacheSizeInMB() {
        return config.get(PRIAM_PRE + ".rowCache.size");
    }

    public String getRowCacheKeysToSave() {
        return config.get(PRIAM_PRE + ".rowCache.count");
    }

    @Override
    public String getCassProcessName() {
        return config.get(PRIAM_PRE + ".cass.process", "CassandraDaemon");
    }

    public String getYamlLocation() {
        return config.get(PRIAM_PRE + ".yamlLocation", getCassHome() + "/conf/cassandra.yaml");
    }

    @Override
    public boolean supportsTuningJVMOptionsFile() {
        return config.get(PRIAM_PRE + ".jvm.options.supported", false);
    }

    @Override
    public String getJVMOptionsFileLocation() {
        return config.get(PRIAM_PRE + ".jvm.options.location", getCassHome() + "/conf/jvm.options");
    }

    public String getAuthenticator() {
        return config.get(
                PRIAM_PRE + ".authenticator", "org.apache.cassandra.auth.AllowAllAuthenticator");
    }

    public String getAuthorizer() {
        return config.get(
                PRIAM_PRE + ".authorizer", "org.apache.cassandra.auth.AllowAllAuthorizer");
    }

    @Override
    public boolean doesCassandraStartManually() {
        return config.get(PRIAM_PRE + ".cass.manual.start.enable", false);
    }

    public String getInternodeCompression() {
        return config.get(PRIAM_PRE + ".internodeCompression", "all");
    }

    @Override
    public boolean isBackingUpCommitLogs() {
        return config.get(PRIAM_PRE + ".clbackup.enabled", false);
    }

    @Override
    public String getCommitLogBackupPropsFile() {
        return config.get(
                PRIAM_PRE + ".clbackup.propsfile",
                getCassHome() + "/conf/commitlog_archiving.properties");
    }

    @Override
    public String getCommitLogBackupArchiveCmd() {
        return config.get(
                PRIAM_PRE + ".clbackup.archiveCmd", "/bin/ln %path /mnt/data/backup/%name");
    }

    @Override
    public String getCommitLogBackupRestoreCmd() {
        return config.get(PRIAM_PRE + ".clbackup.restoreCmd", "/bin/mv %from %to");
    }

    @Override
    public String getCommitLogBackupRestoreFromDirs() {
        return config.get(PRIAM_PRE + ".clbackup.restoreDirs", "/mnt/data/backup/commitlog/");
    }

    @Override
    public String getCommitLogBackupRestorePointInTime() {
        return config.get(PRIAM_PRE + ".clbackup.restoreTime", "");
    }

    @Override
    public int maxCommitLogsRestore() {
        return config.get(PRIAM_PRE + ".clrestore.max", 10);
    }

    public boolean isClientSslEnabled() {
        return config.get(PRIAM_PRE + ".client.sslEnabled", false);
    }

    public String getInternodeEncryption() {
        return config.get(PRIAM_PRE + ".internodeEncryption", "none");
    }

    public boolean isDynamicSnitchEnabled() {
        return config.get(PRIAM_PRE + ".dsnitchEnabled", true);
    }

    public boolean isThriftEnabled() {
        return config.get(PRIAM_PRE + ".thrift.enabled", true);
    }

    public boolean isNativeTransportEnabled() {
        return config.get(PRIAM_PRE + ".nativeTransport.enabled", false);
    }

    public int getConcurrentReadsCnt() {
        return config.get(PRIAM_PRE + ".concurrentReads", 32);
    }

    public int getConcurrentWritesCnt() {
        return config.get(PRIAM_PRE + ".concurrentWrites", 32);
    }

    public int getConcurrentCompactorsCnt() {
        int cpus = Runtime.getRuntime().availableProcessors();
        return config.get(PRIAM_PRE + ".concurrentCompactors", cpus);
    }

    public String getRpcServerType() {
        return config.get(PRIAM_PRE + ".rpc.server.type", "hsha");
    }

    public int getRpcMinThreads() {
        return config.get(PRIAM_PRE + ".rpc.min.threads", 16);
    }

    public int getRpcMaxThreads() {
        return config.get(PRIAM_PRE + ".rpc.max.threads", 2048);
    }

    @Override
    public int getCompactionLargePartitionWarnThresholdInMB() {
        return config.get(PRIAM_PRE + ".compaction.large.partition.warn.threshold", 100);
    }

    public String getExtraConfigParams() {
        return config.get(PRIAM_PRE + ".extra.params");
    }

    @Override
    public Map<String, String> getExtraEnvParams() {

        String envParams = config.get(PRIAM_PRE + ".extra.env.params");
        if (envParams == null) {
            logger.info("getExtraEnvParams: No extra env params");
            return null;
        }
        Map<String, String> extraEnvParamsMap = new HashMap<>();
        String[] pairs = envParams.split(",");
        logger.info("getExtraEnvParams: Extra cass params. From config :{}", envParams);
        for (String pair1 : pairs) {
            String[] pair = pair1.split("=");
            if (pair.length > 1) {
                String priamKey = pair[0];
                String cassKey = pair[1];
                String cassVal = config.get(priamKey);
                logger.info(
                        "getExtraEnvParams: Start-up/ env params: Priamkey[{}], CassStartupKey[{}], Val[{}]",
                        priamKey,
                        cassKey,
                        cassVal);
                if (!StringUtils.isBlank(cassKey) && !StringUtils.isBlank(cassVal)) {
                    extraEnvParamsMap.put(cassKey, cassVal);
                }
            }
        }
        return extraEnvParamsMap;
    }

    public String getCassYamlVal(String priamKey) {
        return config.get(priamKey);
    }

    public boolean getAutoBoostrap() {
        return config.get(PRIAM_PRE + ".auto.bootstrap", true);
    }

    @Override
    public boolean isCreateNewTokenEnable() {
        return config.get(PRIAM_PRE + ".create.new.token.enable", true);
    }

    @Override
    public String getPrivateKeyLocation() {
        return config.get(PRIAM_PRE + ".private.key.location");
    }

    @Override
    public String getRestoreSourceType() {
        return config.get(PRIAM_PRE + ".restore.source.type");
    }

    @Override
    public boolean isEncryptBackupEnabled() {
        return config.get(PRIAM_PRE + ".encrypted.backup.enabled", false);
    }

    @Override
    public String getAWSRoleAssumptionArn() {
        return config.get(PRIAM_PRE + ".roleassumption.arn");
    }

    @Override
    public String getClassicEC2RoleAssumptionArn() {
        return config.get(PRIAM_PRE + ".ec2.roleassumption.arn");
    }

    @Override
    public String getVpcEC2RoleAssumptionArn() {
        return config.get(PRIAM_PRE + ".vpc.roleassumption.arn");
    }

    @Override
    public boolean isDualAccount() {
        return config.get(PRIAM_PRE + ".roleassumption.dualaccount", false);
    }

    @Override
    public String getGcsServiceAccountId() {
        return config.get(PRIAM_PRE + ".gcs.service.acct.id");
    }

    @Override
    public String getGcsServiceAccountPrivateKeyLoc() {
        return config.get(
                PRIAM_PRE + ".gcs.service.acct.private.key",
                "/apps/tomcat/conf/gcsentryptedkey.p12");
    }

    @Override
    public String getPgpPasswordPhrase() {
        return config.get(PRIAM_PRE + ".pgp.password.phrase");
    }

    @Override
    public String getPgpPublicKeyLoc() {
        return config.get(PRIAM_PRE + ".pgp.pubkey.file.location");
    }

    @Override
    public boolean enableAsyncIncremental() {
        return config.get(PRIAM_PRE + ".async.incremental", false);
    }

    @Override
    public boolean enableAsyncSnapshot() {
        return config.get(PRIAM_PRE + ".async.snapshot", false);
    }

    @Override
    public int getBackupQueueSize() {
        return config.get(PRIAM_PRE + ".backup.queue.size", 100000);
    }

    @Override
    public int getDownloadQueueSize() {
        return config.get(PRIAM_PRE + ".download.queue.size", 100000);
    }

    @Override
    public long getUploadTimeout() {
        return config.get(PRIAM_PRE + ".upload.timeout", (2 * 60 * 60 * 1000L));
    }

    public long getDownloadTimeout() {
        return config.get(PRIAM_PRE + ".download.timeout", (10 * 60 * 60 * 1000L));
    }

    @Override
    public int getTombstoneWarnThreshold() {
        return config.get(PRIAM_PRE + ".tombstone.warning.threshold", 1000);
    }

    @Override
    public int getTombstoneFailureThreshold() {
        return config.get(PRIAM_PRE + ".tombstone.failure.threshold", 100000);
    }

    @Override
    public int getStreamingSocketTimeoutInMS() {
        return config.get(PRIAM_PRE + ".streaming.socket.timeout.ms", 86400000);
    }

    @Override
    public String getFlushKeyspaces() {
        return config.get(PRIAM_PRE + ".flush.keyspaces");
    }

    @Override
    public String getBackupStatusFileLoc() {
        return config.get(
                PRIAM_PRE + ".backup.status.location",
                getDataFileLocation() + File.separator + "backup.status");
    }

    @Override
    public boolean useSudo() {
        return config.get(PRIAM_PRE + ".cass.usesudo", true);
    }

    @Override
    public boolean enableBackupNotification() {
        return config.get(PRIAM_PRE + ".enableBackupNotification", true);
    }

    @Override
    public String getBackupNotificationTopicArn() {
        return config.get(PRIAM_PRE + ".backup.notification.topic.arn", "");
    }

    @Override
    public boolean isPostRestoreHookEnabled() {
        return config.get(PRIAM_PRE + ".postrestorehook.enabled", false);
    }

    @Override
    public String getPostRestoreHook() {
        return config.get(PRIAM_PRE + ".postrestorehook");
    }

    @Override
    public String getPostRestoreHookHeartbeatFileName() {
        return config.get(
                PRIAM_PRE + ".postrestorehook.heartbeat.filename",
                getDataFileLocation() + File.separator + "postrestorehook_heartbeat");
    }

    @Override
    public String getPostRestoreHookDoneFileName() {
        return config.get(
                PRIAM_PRE + ".postrestorehook.done.filename",
                getDataFileLocation() + File.separator + "postrestorehook_done");
    }

    @Override
    public int getPostRestoreHookTimeOutInDays() {
        return config.get(PRIAM_PRE + ".postrestorehook.timeout.in.days", 2);
    }

    @Override
    public int getPostRestoreHookHeartBeatTimeoutInMs() {
        return config.get(PRIAM_PRE + ".postrestorehook.heartbeat.timeout", 120000);
    }

    @Override
    public int getPostRestoreHookHeartbeatCheckFrequencyInMs() {
        return config.get(PRIAM_PRE + ".postrestorehook.heartbeat.check.frequency", 120000);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return config.get(key, defaultValue);
    }

    @Override
    public String getMergedConfigurationCronExpression() {
        // Every minute on the top of the minute.
        return config.get(PRIAM_PRE + ".configMerge.cron", "0 * * * * ? *");
    }

    @Override
    public int getGracePeriodDaysForCompaction() {
        return config.get(PRIAM_PRE + ".gracePeriodDaysForCompaction", 5);
    }

    @Override
    public int getForgottenFileGracePeriodDaysForRead() {
        return config.get(PRIAM_PRE + ".forgottenFileGracePeriodDaysForRead", 3);
    }

    @Override
    public boolean isForgottenFileMoveEnabled() {
        return config.get(PRIAM_PRE + ".forgottenFileMoveEnabled", false);
    }

    @Override
    public boolean checkThriftServerIsListening() {
        return config.get(PRIAM_PRE + ".checkThriftServerIsListening", false);
    }

    @Override
    public BackupsToCompress getBackupsToCompress() {
        return BackupsToCompress.valueOf(
                config.get("priam.backupsToCompress", BackupsToCompress.ALL.name()));
    }

    @Override
    public boolean permitDirectTokenAssignmentWithGossipMismatch() {
        return config.get(PRIAM_PRE + ".permitDirectTokenAssignmentWithGossipMismatch", false);
    }

    @Override
    public int getTargetMinutesToCompleteSnaphotUpload() {
        return config.get(PRIAM_PRE + ".snapshotUploadDuration", 0);
    }

    @Override
    public double getRateLimitChangeThreshold() {
        return config.get(PRIAM_PRE + ".rateLimitChangeThreshold", 0.1);
    }

    @Override
    public boolean getAutoSnapshot() {
        return config.get(PRIAM_PRE + ".autoSnapshot", false);
    }
}
