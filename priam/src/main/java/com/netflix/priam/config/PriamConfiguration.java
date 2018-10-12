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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.configSource.IConfigSource;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.identity.InstanceEnvIdentity;
import com.netflix.priam.identity.config.InstanceDataRetriever;
import com.netflix.priam.scheduler.SchedulerType;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.tuner.GCType;
import com.netflix.priam.tuner.JVMOption;
import com.netflix.priam.tuner.JVMOptionsTuner;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.SystemUtils;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PriamConfiguration implements IConfiguration {
    public static final String PRIAM_PRE = "priam";

    private static final String CONFIG_CASS_HOME_DIR = PRIAM_PRE + ".cass.home";
    private static final String CONFIG_CASS_START_SCRIPT = PRIAM_PRE + ".cass.startscript";
    private static final String CONFIG_CASS_STOP_SCRIPT = PRIAM_PRE + ".cass.stopscript";
    private static final String CONFIG_CASS_USE_SUDO = PRIAM_PRE + ".cass.usesudo";
    private static final String CONFIG_CLUSTER_NAME = PRIAM_PRE + ".clustername";
    private static final String CONFIG_SEED_PROVIDER_NAME = PRIAM_PRE + ".seed.provider";
    private static final String CONFIG_LOAD_LOCAL_PROPERTIES = PRIAM_PRE + ".localbootstrap.enable";
    private static final String CONFIG_MAX_HEAP_SIZE = PRIAM_PRE + ".heap.size.";
    private static final String CONFIG_DATA_LOCATION = PRIAM_PRE + ".data.location";
    private static final String CONFIG_LOGS_LOCATION = PRIAM_PRE + ".logs.location";
    private static final String CONFIG_MR_ENABLE = PRIAM_PRE + ".multiregion.enable";
    private static final String CONFIG_CL_LOCATION = PRIAM_PRE + ".commitlog.location";
    private static final String CONFIG_JMX_LISTERN_PORT_NAME = PRIAM_PRE + ".jmx.port";
    private static final String CONFIG_JMX_USERNAME = PRIAM_PRE + ".jmx.username";
    private static final String CONFIG_JMX_PASSWORD = PRIAM_PRE + ".jmx.password";
    private static final String CONFIG_JMX_ENABLE_REMOTE = PRIAM_PRE + ".jmx.remote.enable";
    private static final String CONFIG_AVAILABILITY_ZONES = PRIAM_PRE + ".zones.available";
    private static final String CONFIG_SAVE_CACHE_LOCATION = PRIAM_PRE + ".cache.location";
    private static final String CONFIG_NEW_MAX_HEAP_SIZE = PRIAM_PRE + ".heap.newgen.size.";
    private static final String CONFIG_DIRECT_MAX_HEAP_SIZE = PRIAM_PRE + ".direct.memory.size.";
    private static final String CONFIG_THRIFT_LISTEN_PORT_NAME = PRIAM_PRE + ".thrift.port";
    private static final String CONFIG_THRIFT_ENABLED = PRIAM_PRE + ".thrift.enabled";
    private static final String CONFIG_NATIVE_PROTOCOL_PORT = PRIAM_PRE + ".nativeTransport.port";
    private static final String CONFIG_NATIVE_PROTOCOL_ENABLED =
            PRIAM_PRE + ".nativeTransport.enabled";
    private static final String CONFIG_STORAGE_LISTERN_PORT_NAME = PRIAM_PRE + ".storage.port";
    private static final String CONFIG_SSL_STORAGE_LISTERN_PORT_NAME =
            PRIAM_PRE + ".ssl.storage.port";
    private static final String CONFIG_CL_BK_LOCATION = PRIAM_PRE + ".backup.commitlog.location";
    private static final String CONFIG_THROTTLE_UPLOAD_PER_SECOND = PRIAM_PRE + ".upload.throttle";
    private static final String CONFIG_COMPACTION_THROUHPUT = PRIAM_PRE + ".compaction.throughput";
    private static final String CONFIG_MAX_HINT_WINDOW_IN_MS = PRIAM_PRE + ".hint.window";
    private static final String CONFIG_BOOTCLUSTER_NAME = PRIAM_PRE + ".bootcluster";
    private static final String CONFIG_ENDPOINT_SNITCH = PRIAM_PRE + ".endpoint_snitch";
    private static final String CONFIG_MEMTABLE_CLEANUP_THRESHOLD =
            PRIAM_PRE + ".memtable.cleanup.threshold";
    private static final String CONFIG_CASS_PROCESS_NAME = PRIAM_PRE + ".cass.process";
    private static final String CONFIG_VNODE_NUM_TOKENS = PRIAM_PRE + ".vnodes.numTokens";
    private static final String CONFIG_YAML_LOCATION = PRIAM_PRE + ".yamlLocation";
    private static final String CONFIG_AUTHENTICATOR = PRIAM_PRE + ".authenticator";
    private static final String CONFIG_AUTHORIZER = PRIAM_PRE + ".authorizer";
    private static final String CONFIG_CASS_MANUAL_START_ENABLE =
            PRIAM_PRE + ".cass.manual.start.enable";
    private static final String CONFIG_REMEDIATE_DEAD_CASSANDRA_RATE_S =
            PRIAM_PRE + ".remediate.dead.cassandra.rate";
    private static final String CONFIG_CREATE_NEW_TOKEN_ENABLE =
            PRIAM_PRE + ".create.new.token.enable";

    // Backup and Restore
    private static final String CONFIG_BACKUP_THREADS = PRIAM_PRE + ".backup.threads";
    private static final String CONFIG_RESTORE_PREFIX = PRIAM_PRE + ".restore.prefix";
    private static final String CONFIG_INCR_BK_ENABLE = PRIAM_PRE + ".backup.incremental.enable";

    private static final String CONFIG_AUTO_RESTORE_SNAPSHOTNAME = PRIAM_PRE + ".restore.snapshot";
    private static final String CONFIG_BUCKET_NAME = PRIAM_PRE + ".s3.bucket";
    private static final String CONFIG_BACKUP_SCHEDULE_TYPE = PRIAM_PRE + ".backup.schedule.type";
    private static final String CONFIG_BACKUP_HOUR = PRIAM_PRE + ".backup.hour";
    private static final String CONFIG_BACKUP_CRON_EXPRESSION = PRIAM_PRE + ".backup.cron";
    private static final String CONFIG_S3_BASE_DIR = PRIAM_PRE + ".s3.base_dir";
    private static final String CONFIG_RESTORE_THREADS = PRIAM_PRE + ".restore.threads";
    private static final String CONFIG_RESTORE_CLOSEST_TOKEN = PRIAM_PRE + ".restore.closesttoken";
    private static final String CONFIG_BACKUP_CHUNK_SIZE = PRIAM_PRE + ".backup.chunksizemb";
    private static final String CONFIG_BACKUP_RETENTION = PRIAM_PRE + ".backup.retention";
    private static final String CONFIG_BACKUP_RACS = PRIAM_PRE + ".backup.racs";
    private static final String CONFIG_BACKUP_STATUS_FILE_LOCATION =
            PRIAM_PRE + ".backup.status.location";
    private static final String CONFIG_STREAMING_THROUGHPUT_MB =
            PRIAM_PRE + ".streaming.throughput.mb";
    private static final String CONFIG_STREAMING_SOCKET_TIMEOUT_IN_MS =
            PRIAM_PRE + ".streaming.socket.timeout.ms";
    private static final String CONFIG_TOMBSTONE_FAILURE_THRESHOLD =
            PRIAM_PRE + ".tombstone.failure.threshold";
    private static final String CONFIG_TOMBSTONE_WARNING_THRESHOLD =
            PRIAM_PRE + ".tombstone.warning.threshold";

    private static final String CONFIG_PARTITIONER = PRIAM_PRE + ".partitioner";
    private static final String CONFIG_KEYCACHE_SIZE = PRIAM_PRE + ".keyCache.size";
    private static final String CONFIG_KEYCACHE_COUNT = PRIAM_PRE + ".keyCache.count";
    private static final String CONFIG_ROWCACHE_SIZE = PRIAM_PRE + ".rowCache.size";
    private static final String CONFIG_ROWCACHE_COUNT = PRIAM_PRE + ".rowCache.count";

    private static final String CONFIG_MAX_HINT_THREADS = PRIAM_PRE + ".hints.maxThreads";
    private static final String CONFIG_HINTS_THROTTLE_KB = PRIAM_PRE + ".hints.throttleKb";
    private static final String CONFIG_INTERNODE_COMPRESSION = PRIAM_PRE + ".internodeCompression";

    private static final String CONFIG_COMMITLOG_BKUP_ENABLED = PRIAM_PRE + ".clbackup.enabled";
    private static final String CONFIG_COMMITLOG_PROPS_FILE = PRIAM_PRE + ".clbackup.propsfile";
    private static final String CONFIG_COMMITLOG_ARCHIVE_CMD = PRIAM_PRE + ".clbackup.archiveCmd";
    private static final String CONFIG_COMMITLOG_RESTORE_CMD = PRIAM_PRE + ".clbackup.restoreCmd";
    private static final String CONFIG_COMMITLOG_RESTORE_DIRS = PRIAM_PRE + ".clbackup.restoreDirs";
    private static final String CONFIG_COMMITLOG_RESTORE_POINT_IN_TIME =
            PRIAM_PRE + ".clbackup.restoreTime";
    private static final String CONFIG_COMMITLOG_RESTORE_MAX = PRIAM_PRE + ".clrestore.max";
    private static final String CONFIG_CLIENT_SSL_ENABLED = PRIAM_PRE + ".client.sslEnabled";
    private static final String CONFIG_INTERNODE_ENCRYPTION = PRIAM_PRE + ".internodeEncryption";
    private static final String CONFIG_DSNITCH_ENABLED = PRIAM_PRE + ".dsnitchEnabled";

    private static final String CONFIG_CONCURRENT_READS = PRIAM_PRE + ".concurrentReads";
    private static final String CONFIG_CONCURRENT_WRITES = PRIAM_PRE + ".concurrentWrites";
    private static final String CONFIG_CONCURRENT_COMPACTORS = PRIAM_PRE + ".concurrentCompactors";

    private static final String CONFIG_RPC_SERVER_TYPE = PRIAM_PRE + ".rpc.server.type";
    private static final String CONFIG_RPC_MIN_THREADS = PRIAM_PRE + ".rpc.min.threads";
    private static final String CONFIG_RPC_MAX_THREADS = PRIAM_PRE + ".rpc.max.threads";
    private static final String CONFIG_EXTRA_PARAMS = PRIAM_PRE + ".extra.params";
    private static final String CONFIG_AUTO_BOOTSTRAP = PRIAM_PRE + ".auto.bootstrap";
    private static final String CONFIG_EXTRA_ENV_PARAMS = PRIAM_PRE + ".extra.env.params";

    private static final String CONFIG_RESTORE_SOURCE_TYPE =
            PRIAM_PRE + ".restore.source.type"; // the type of source for the restore.  Valid values
    // are: AWSCROSSACCT or GOOGLE.
    private static final String CONFIG_ENCRYPTED_BACKUP_ENABLED =
            PRIAM_PRE + ".encrypted.backup.enabled"; // enable encryption of backup (snapshots,
    // incrementals, commit logs).

    // Backup and restore cryptography
    private static final String CONFIG_PRIKEY_LOC =
            PRIAM_PRE + ".private.key.location"; // the location on disk of the private key used by
    // the cryptography algorithm
    private static final String CONFIG_PGP_PASSWORD_PHRASE =
            PRIAM_PRE + ".pgp.password.phrase"; // pass phrase used by the cryptography algorithm
    private static final String CONFIG_PGP_PUB_KEY_LOC = PRIAM_PRE + ".pgp.pubkey.file.location";

    // Restore from Google Cloud Storage
    private static final String CONFIG_GCS_SERVICE_ACCT_ID =
            PRIAM_PRE + ".gcs.service.acct.id"; // Google Cloud Storage service account id
    private static final String CONFIG_GCS_SERVICE_ACCT_PRIVATE_KEY_LOC =
            PRIAM_PRE + ".gcs.service.acct.private.key"; // the absolute path on disk for the Google
    // Cloud Storage PFX file (i.e. the combined
    // format of the private key and
    // certificate).

    // Amazon specific
    private static final String CONFIG_ASG_NAME = PRIAM_PRE + ".az.asgname";
    private static final String CONFIG_SIBLING_ASG_NAMES = PRIAM_PRE + ".az.sibling.asgnames";
    private static final String CONFIG_REGION_NAME = PRIAM_PRE + ".az.region";
    private static final String SDB_INSTANCE_INDENTITY_REGION_NAME =
            PRIAM_PRE + ".sdb.instanceIdentity.region";
    private static final String CONFIG_ACL_GROUP_NAME = PRIAM_PRE + ".acl.groupname";
    private static String ASG_NAME = System.getenv("ASG_NAME");
    private static String REGION = System.getenv("EC2_REGION");
    private static final String CONFIG_VPC_RING = PRIAM_PRE + ".vpc";
    private static final String CONFIG_S3_ROLE_ASSUMPTION_ARN =
            PRIAM_PRE
                    + ".roleassumption.arn"; // Restore from AWS.  This is applicable when restoring
    // from an AWS account which requires cross account
    // assumption.
    private static final String CONFIG_EC2_ROLE_ASSUMPTION_ARN =
            PRIAM_PRE + ".ec2.roleassumption.arn";
    private static final String CONFIG_VPC_ROLE_ASSUMPTION_ARN =
            PRIAM_PRE + ".vpc.roleassumption.arn";
    private static final String CONFIG_DUAL_ACCOUNT = PRIAM_PRE + ".roleassumption.dualaccount";

    // Post Restore Hook
    private static final String CONFIG_POST_RESTORE_HOOK_ENABLED =
            PRIAM_PRE + ".postrestorehook.enabled";
    private static final String CONFIG_POST_RESTORE_HOOK = PRIAM_PRE + ".postrestorehook";
    private static final String CONFIG_POST_RESTORE_HOOK_HEARTBEAT_FILENAME =
            PRIAM_PRE + ".postrestorehook.heartbeat.filename";
    private static final String CONFIG_POST_RESTORE_HOOK_DONE_FILENAME =
            PRIAM_PRE + ".postrestorehook.done.filename";
    private static final String CONFIG_POST_RESTORE_HOOK_TIMEOUT_IN_DAYS =
            PRIAM_PRE + ".postrestorehook.timeout.in.days";
    private static final String CONFIG_POST_RESTORE_HOOK_HEARTBEAT_TIMEOUT_MS =
            PRIAM_PRE + ".postrestorehook.heartbeat.timeout";
    private static final String CONFIG_POST_RESTORE_HOOK_HEARTBEAT_CHECK_FREQUENCY_MS =
            PRIAM_PRE + ".postrestorehook.heartbeat.check.frequency";

    // Running instance meta data
    private String RAC;
    private String INSTANCE_ID;

    // == vpc specific
    private String NETWORK_VPC; // Fetch the vpc id of running instance

    private final String CASS_BASE_DATA_DIR = "/var/lib/cassandra";
    public static final String DEFAULT_AUTHENTICATOR =
            "org.apache.cassandra.auth.AllowAllAuthenticator";
    public static final String DEFAULT_AUTHORIZER = "org.apache.cassandra.auth.AllowAllAuthorizer";
    public static final String DEFAULT_COMMITLOG_PROPS_FILE =
            "/conf/commitlog_archiving.properties";

    //    private String DEFAULT_AVAILABILITY_ZONES = "";
    private List<String> DEFAULT_AVAILABILITY_ZONES = ImmutableList.of();

    private final int DEFAULT_HINTS_MAX_THREADS = 2; // default value from 1.2 yaml

    private static final String DEFAULT_RPC_SERVER_TYPE = "hsha";
    private static final int DEFAULT_RPC_MIN_THREADS = 16;
    private static final int DEFAULT_RPC_MAX_THREADS = 2048;
    private static final int DEFAULT_STREAMING_SOCKET_TIMEOUT_IN_MS = 86400000; // 24 Hours
    private static final int DEFAULT_TOMBSTONE_WARNING_THRESHOLD = 1000; // C* defaults
    private static final int DEFAULT_TOMBSTONE_FAILURE_THRESHOLD = 100000; // C* defaults

    // AWS EC2 Dual Account
    private static final boolean DEFAULT_DUAL_ACCOUNT = false;

    private final IConfigSource config;
    private static final Logger logger = LoggerFactory.getLogger(PriamConfiguration.class);
    private final ICredential provider;

    @JsonIgnore private final InstanceEnvIdentity insEnvIdentity;
    @JsonIgnore private InstanceDataRetriever instanceDataRetriever;

    @Inject
    public PriamConfiguration(
            ICredential provider, IConfigSource config, InstanceEnvIdentity insEnvIdentity) {
        this.provider = provider;
        this.config = config;
        this.insEnvIdentity = insEnvIdentity;
    }

    @Override
    public void initialize() {
        try {
            if (this.insEnvIdentity.isClassic()) {
                this.instanceDataRetriever =
                        (InstanceDataRetriever)
                                Class.forName(
                                                "com.netflix.priam.identity.config.AwsClassicInstanceDataRetriever")
                                        .newInstance();

            } else if (this.insEnvIdentity.isNonDefaultVpc()) {
                this.instanceDataRetriever =
                        (InstanceDataRetriever)
                                Class.forName(
                                                "com.netflix.priam.identity.config.AWSVpcInstanceDataRetriever")
                                        .newInstance();
            } else {
                throw new IllegalStateException(
                        "Unable to determine environemt (vpc, classic) for running instance.");
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Exception when instantiating the instance data retriever.  Msg: "
                            + e.getLocalizedMessage());
        }

        RAC = instanceDataRetriever.getRac();
        INSTANCE_ID = instanceDataRetriever.getInstanceId();

        NETWORK_VPC = instanceDataRetriever.getVpcId();

        setupEnvVars();
        this.config.intialize(ASG_NAME, REGION);
        setDefaultRACList(REGION);
        populateProps();
        SystemUtils.createDirs(getBackupCommitLogLocation());
        SystemUtils.createDirs(getCommitLogLocation());
        SystemUtils.createDirs(getCacheLocation());
        SystemUtils.createDirs(getDataFileLocation());
        SystemUtils.createDirs(getHintsLocation());
        SystemUtils.createDirs(getLogDirLocation());
    }

    public InstanceDataRetriever getInstanceDataRetriever() {
        return instanceDataRetriever;
    }

    private void setupEnvVars() {
        // Search in java opt properties
        REGION = StringUtils.isBlank(REGION) ? System.getProperty("EC2_REGION") : REGION;
        // Infer from zone
        if (StringUtils.isBlank(REGION)) REGION = RAC.substring(0, RAC.length() - 1);
        ASG_NAME = StringUtils.isBlank(ASG_NAME) ? System.getProperty("ASG_NAME") : ASG_NAME;
        if (StringUtils.isBlank(ASG_NAME))
            ASG_NAME = populateASGName(REGION, getInstanceDataRetriever().getInstanceId());
        logger.info("REGION set to {}, ASG Name set to {}", REGION, ASG_NAME);
    }

    /** Query amazon to get ASG name. Currently not available as part of instance info api. */
    private String populateASGName(String region, String instanceId) {
        GetASGName getASGName = new GetASGName(region, instanceId);

        try {
            return getASGName.call();
        } catch (Exception e) {
            logger.error("Failed to determine ASG name.", e);
            return null;
        }
    }

    private class GetASGName extends RetryableCallable<String> {
        private static final int NUMBER_OF_RETRIES = 15;
        private static final long WAIT_TIME = 30000;
        private final String region;
        private final String instanceId;
        private final AmazonEC2 client;

        public GetASGName(String region, String instanceId) {
            super(NUMBER_OF_RETRIES, WAIT_TIME);
            this.region = region;
            this.instanceId = instanceId;
            client =
                    AmazonEC2ClientBuilder.standard()
                            .withCredentials(provider.getAwsCredentialProvider())
                            .withRegion(region)
                            .build();
        }

        @Override
        public String retriableCall() throws IllegalStateException {
            DescribeInstancesRequest desc =
                    new DescribeInstancesRequest().withInstanceIds(instanceId);
            DescribeInstancesResult res = client.describeInstances(desc);

            for (Reservation resr : res.getReservations()) {
                for (Instance ins : resr.getInstances()) {
                    for (com.amazonaws.services.ec2.model.Tag tag : ins.getTags()) {
                        if (tag.getKey().equals("aws:autoscaling:groupName")) return tag.getValue();
                    }
                }
            }

            logger.warn("Couldn't determine ASG name");
            throw new IllegalStateException("Couldn't determine ASG name");
        }
    }

    /** Get the fist 3 available zones in the region */
    public void setDefaultRACList(String region) {
        AmazonEC2 client =
                AmazonEC2ClientBuilder.standard()
                        .withCredentials(provider.getAwsCredentialProvider())
                        .withRegion(region)
                        .build();
        DescribeAvailabilityZonesResult res = client.describeAvailabilityZones();
        List<String> zone = Lists.newArrayList();
        for (AvailabilityZone reg : res.getAvailabilityZones()) {
            if (reg.getState().equals("available")) zone.add(reg.getZoneName());
            if (zone.size() == 3) break;
        }
        DEFAULT_AVAILABILITY_ZONES = ImmutableList.copyOf(zone);
    }

    private void populateProps() {
        config.set(CONFIG_ASG_NAME, ASG_NAME);
        config.set(CONFIG_REGION_NAME, REGION);
    }

    public String getInstanceName() {
        return INSTANCE_ID;
    }

    @Override
    public String getCassStartupScript() {
        return config.get(CONFIG_CASS_START_SCRIPT, "/etc/init.d/cassandra start");
    }

    @Override
    public String getCassStopScript() {
        return config.get(CONFIG_CASS_STOP_SCRIPT, "/etc/init.d/cassandra stop");
    }

    @Override
    public int getGracefulDrainHealthWaitSeconds() {
        return -1;
    }

    @Override
    public int getRemediateDeadCassandraRate() {
        return config.get(CONFIG_REMEDIATE_DEAD_CASSANDRA_RATE_S, 3600); // Default to once per hour
    }

    @Override
    public String getCassHome() {
        return config.get(CONFIG_CASS_HOME_DIR, "/etc/cassandra");
    }

    @Override
    public String getBackupLocation() {
        return config.get(CONFIG_S3_BASE_DIR, "backup");
    }

    @Override
    public String getBackupPrefix() {
        return config.get(CONFIG_BUCKET_NAME, "cassandra-archive");
    }

    @Override
    public int getBackupRetentionDays() {
        return config.get(CONFIG_BACKUP_RETENTION, 0);
    }

    @Override
    public List<String> getBackupRacs() {
        return config.getList(CONFIG_BACKUP_RACS);
    }

    @Override
    public String getRestorePrefix() {
        return config.get(CONFIG_RESTORE_PREFIX);
    }

    @Override
    public String getDataFileLocation() {
        return config.get(CONFIG_DATA_LOCATION, CASS_BASE_DATA_DIR + "/data");
    }

    @Override
    public String getLogDirLocation() {
        return config.get(CONFIG_LOGS_LOCATION, CASS_BASE_DATA_DIR + "/logs");
    }

    @Override
    public String getHintsLocation() {
        return config.get(PRIAM_PRE + ".hints.location", CASS_BASE_DATA_DIR + "/hints");
    }

    @Override
    public String getCacheLocation() {
        return config.get(CONFIG_SAVE_CACHE_LOCATION, CASS_BASE_DATA_DIR + "/saved_caches");
    }

    @Override
    public String getCommitLogLocation() {
        return config.get(CONFIG_CL_LOCATION, CASS_BASE_DATA_DIR + "/commitlog");
    }

    @Override
    public String getBackupCommitLogLocation() {
        return config.get(CONFIG_CL_BK_LOCATION, "");
    }

    @Override
    public long getBackupChunkSize() {
        long size = config.get(CONFIG_BACKUP_CHUNK_SIZE, 10);
        return size * 1024 * 1024L;
    }

    @Override
    public int getJmxPort() {
        return config.get(CONFIG_JMX_LISTERN_PORT_NAME, 7199);
    }

    @Override
    public String getJmxUsername() {
        return config.get(CONFIG_JMX_USERNAME, "");
    }

    @Override
    public String getJmxPassword() {
        return config.get(CONFIG_JMX_PASSWORD, "");
    }

    /** @return Enables Remote JMX connections n C* */
    @Override
    public boolean enableRemoteJMX() {
        return config.get(CONFIG_JMX_ENABLE_REMOTE, false);
    }

    public int getNativeTransportPort() {
        return config.get(CONFIG_NATIVE_PROTOCOL_PORT, 9042);
    }

    @Override
    public int getThriftPort() {
        return config.get(CONFIG_THRIFT_LISTEN_PORT_NAME, 9160);
    }

    @Override
    public int getStoragePort() {
        return config.get(CONFIG_STORAGE_LISTERN_PORT_NAME, 7000);
    }

    @Override
    public int getSSLStoragePort() {
        return config.get(CONFIG_SSL_STORAGE_LISTERN_PORT_NAME, 7001);
    }

    @Override
    public String getSnitch() {
        return config.get(CONFIG_ENDPOINT_SNITCH, "org.apache.cassandra.locator.Ec2Snitch");
    }

    @Override
    public String getAppName() {
        return config.get(CONFIG_CLUSTER_NAME, "cass_cluster");
    }

    @Override
    public String getRac() {
        return RAC;
    }

    @Override
    public List<String> getRacs() {
        return config.getList(CONFIG_AVAILABILITY_ZONES, DEFAULT_AVAILABILITY_ZONES);
    }

    @JsonIgnore
    @Override
    public String getHostname() {
        if (this.isVpcRing()) return getInstanceDataRetriever().getPrivateIP();
        else return getInstanceDataRetriever().getPublicHostname();
    }

    @Override
    public String getHeapSize() {
        return config.get(
                CONFIG_MAX_HEAP_SIZE + getInstanceDataRetriever().getInstanceType(), "8G");
    }

    @Override
    public String getHeapNewSize() {
        return config.get(
                CONFIG_NEW_MAX_HEAP_SIZE + getInstanceDataRetriever().getInstanceType(), "2G");
    }

    @Override
    public String getMaxDirectMemory() {
        return config.get(
                CONFIG_DIRECT_MAX_HEAP_SIZE + getInstanceDataRetriever().getInstanceType(), "50G");
    }

    @Override
    public int getBackupHour() {
        return config.get(CONFIG_BACKUP_HOUR, 12);
    }

    @Override
    public String getBackupCronExpression() {
        return config.get(CONFIG_BACKUP_CRON_EXPRESSION, "0 0 12 1/1 * ? *"); // Backup daily at 12
    }

    @Override
    public SchedulerType getBackupSchedulerType() throws UnsupportedTypeException {
        String schedulerType =
                config.get(CONFIG_BACKUP_SCHEDULE_TYPE, SchedulerType.HOUR.getSchedulerType());
        return SchedulerType.lookup(schedulerType);
    }

    @Override
    public GCType getGCType() throws UnsupportedTypeException {
        String gcType = config.get(PRIAM_PRE + ".gc.type", GCType.CMS.getGcType());
        return GCType.lookup(gcType);
    }

    @Override
    public Map<String, JVMOption> getJVMExcludeSet() {
        return JVMOptionsTuner.parseJVMOptions(config.get(PRIAM_PRE + ".jvm.options.exclude"));
    }

    @Override
    public Map<String, JVMOption> getJVMUpsertSet() {
        return JVMOptionsTuner.parseJVMOptions(config.get(PRIAM_PRE + ".jvm.options.upsert"));
    }

    @Override
    public SchedulerType getFlushSchedulerType() throws UnsupportedTypeException {
        String schedulerType =
                config.get(
                        PRIAM_PRE + ".flush.schedule.type", SchedulerType.HOUR.getSchedulerType());
        return SchedulerType.lookup(schedulerType);
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
        return config.get(CONFIG_AUTO_RESTORE_SNAPSHOTNAME, "");
    }

    @Override
    public boolean isRestoreEncrypted() {
        return config.get(PRIAM_PRE + ".encrypted.restore.enabled", false);
    }

    @Override
    public String getSDBInstanceIdentityRegion() {
        return config.get(SDB_INSTANCE_INDENTITY_REGION_NAME, "us-east-1");
    }

    @Override
    public String getDC() {
        return config.get(CONFIG_REGION_NAME, "");
    }

    @Override
    public void setDC(String region) {
        config.set(CONFIG_REGION_NAME, region);
    }

    @Override
    public boolean isMultiDC() {
        return config.get(CONFIG_MR_ENABLE, false);
    }

    @Override
    public int getBackupThreads() {
        return config.get(CONFIG_BACKUP_THREADS, 2);
    }

    @Override
    public int getRestoreThreads() {
        return config.get(CONFIG_RESTORE_THREADS, 8);
    }

    @Override
    public boolean isRestoreClosestToken() {
        return config.get(CONFIG_RESTORE_CLOSEST_TOKEN, false);
    }

    @Override
    public String getASGName() {
        return config.get(CONFIG_ASG_NAME, "");
    }

    /**
     * Amazon specific setting to query Additional/ Sibling ASG Memberships in csv format to
     * consider while calculating RAC membership
     */
    @Override
    public String getSiblingASGNames() {
        return config.get(CONFIG_SIBLING_ASG_NAMES, ",");
    }

    @Override
    public String getACLGroupName() {
        return config.get(CONFIG_ACL_GROUP_NAME, this.getAppName());
    }

    @Override
    public boolean isIncrBackup() {
        return config.get(CONFIG_INCR_BK_ENABLE, true);
    }

    @Override
    public String getHostIP() {
        if (this.isVpcRing()) return getInstanceDataRetriever().getPrivateIP();
        else return getInstanceDataRetriever().getPublicIP();
    }

    @Override
    public int getUploadThrottle() {
        return config.get(CONFIG_THROTTLE_UPLOAD_PER_SECOND, Integer.MAX_VALUE);
    }

    @Override
    public boolean isLocalBootstrapEnabled() {
        return config.get(CONFIG_LOAD_LOCAL_PROPERTIES, false);
    }

    @Override
    public int getCompactionThroughput() {
        return config.get(CONFIG_COMPACTION_THROUHPUT, 8);
    }

    @Override
    public int getMaxHintWindowInMS() {
        return config.get(CONFIG_MAX_HINT_WINDOW_IN_MS, 10800000);
    }

    public int getHintedHandoffThrottleKb() {
        return config.get(CONFIG_HINTS_THROTTLE_KB, 1024);
    }

    @Override
    public String getBootClusterName() {
        return config.get(CONFIG_BOOTCLUSTER_NAME, "");
    }

    @Override
    public String getSeedProviderName() {
        return config.get(
                CONFIG_SEED_PROVIDER_NAME, "com.netflix.priam.cassandra.extensions.NFSeedProvider");
    }

    public double getMemtableCleanupThreshold() {
        return config.get(CONFIG_MEMTABLE_CLEANUP_THRESHOLD, 0.11);
    }

    @Override
    public int getStreamingThroughputMB() {
        return config.get(CONFIG_STREAMING_THROUGHPUT_MB, 400);
    }

    public String getPartitioner() {
        return config.get(CONFIG_PARTITIONER, "org.apache.cassandra.dht.RandomPartitioner");
    }

    public String getKeyCacheSizeInMB() {
        return config.get(CONFIG_KEYCACHE_SIZE);
    }

    public String getKeyCacheKeysToSave() {
        return config.get(CONFIG_KEYCACHE_COUNT);
    }

    public String getRowCacheSizeInMB() {
        return config.get(CONFIG_ROWCACHE_SIZE);
    }

    public String getRowCacheKeysToSave() {
        return config.get(CONFIG_ROWCACHE_COUNT);
    }

    @Override
    public String getCassProcessName() {
        return config.get(CONFIG_CASS_PROCESS_NAME, "CassandraDaemon");
    }

    public int getNumTokens() {
        return config.get(CONFIG_VNODE_NUM_TOKENS, 1);
    }

    public String getYamlLocation() {
        return config.get(CONFIG_YAML_LOCATION, getCassHome() + "/conf/cassandra.yaml");
    }

    @Override
    public String getJVMOptionsFileLocation() {
        return config.get(PRIAM_PRE + ".jvm.options.location", getCassHome() + "/conf/jvm.options");
    }

    public String getAuthenticator() {
        return config.get(CONFIG_AUTHENTICATOR, DEFAULT_AUTHENTICATOR);
    }

    public String getAuthorizer() {
        return config.get(CONFIG_AUTHORIZER, DEFAULT_AUTHORIZER);
    }

    @Override
    public boolean doesCassandraStartManually() {
        return config.get(CONFIG_CASS_MANUAL_START_ENABLE, false);
    }

    public String getInternodeCompression() {
        return config.get(CONFIG_INTERNODE_COMPRESSION, "all");
    }

    @Override
    public void setRestorePrefix(String prefix) {
        config.set(CONFIG_RESTORE_PREFIX, prefix);
    }

    @Override
    public boolean isBackingUpCommitLogs() {
        return config.get(CONFIG_COMMITLOG_BKUP_ENABLED, false);
    }

    @Override
    public String getCommitLogBackupPropsFile() {
        return config.get(
                CONFIG_COMMITLOG_PROPS_FILE, getCassHome() + DEFAULT_COMMITLOG_PROPS_FILE);
    }

    @Override
    public String getCommitLogBackupArchiveCmd() {
        return config.get(CONFIG_COMMITLOG_ARCHIVE_CMD, "/bin/ln %path /mnt/data/backup/%name");
    }

    @Override
    public String getCommitLogBackupRestoreCmd() {
        return config.get(CONFIG_COMMITLOG_RESTORE_CMD, "/bin/mv %from %to");
    }

    @Override
    public String getCommitLogBackupRestoreFromDirs() {
        return config.get(CONFIG_COMMITLOG_RESTORE_DIRS, "/mnt/data/backup/commitlog/");
    }

    @Override
    public String getCommitLogBackupRestorePointInTime() {
        return config.get(CONFIG_COMMITLOG_RESTORE_POINT_IN_TIME, "");
    }

    @Override
    public int maxCommitLogsRestore() {
        return config.get(CONFIG_COMMITLOG_RESTORE_MAX, 10);
    }

    @Override
    public boolean isVpcRing() {
        return config.get(CONFIG_VPC_RING, false);
    }

    public boolean isClientSslEnabled() {
        return config.get(CONFIG_CLIENT_SSL_ENABLED, false);
    }

    public String getInternodeEncryption() {
        return config.get(CONFIG_INTERNODE_ENCRYPTION, "none");
    }

    public boolean isDynamicSnitchEnabled() {
        return config.get(CONFIG_DSNITCH_ENABLED, true);
    }

    public boolean isThriftEnabled() {
        return config.get(CONFIG_THRIFT_ENABLED, true);
    }

    public boolean isNativeTransportEnabled() {
        return config.get(CONFIG_NATIVE_PROTOCOL_ENABLED, false);
    }

    public int getConcurrentReadsCnt() {
        return config.get(CONFIG_CONCURRENT_READS, 32);
    }

    public int getConcurrentWritesCnt() {
        return config.get(CONFIG_CONCURRENT_WRITES, 32);
    }

    public int getConcurrentCompactorsCnt() {
        int cpus = Runtime.getRuntime().availableProcessors();
        return config.get(CONFIG_CONCURRENT_COMPACTORS, cpus);
    }

    public String getRpcServerType() {
        return config.get(CONFIG_RPC_SERVER_TYPE, DEFAULT_RPC_SERVER_TYPE);
    }

    public int getRpcMinThreads() {
        return config.get(CONFIG_RPC_MIN_THREADS, DEFAULT_RPC_MIN_THREADS);
    }

    public int getRpcMaxThreads() {
        return config.get(CONFIG_RPC_MAX_THREADS, DEFAULT_RPC_MAX_THREADS);
    }

    @Override
    public int getCompactionLargePartitionWarnThresholdInMB() {
        return config.get(PRIAM_PRE + ".compaction.large.partition.warn.threshold", 100);
    }

    public String getExtraConfigParams() {
        return config.get(CONFIG_EXTRA_PARAMS);
    }

    public Map<String, String> getExtraEnvParams() {

        String envParams = config.get(CONFIG_EXTRA_ENV_PARAMS);
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
        return config.get(CONFIG_AUTO_BOOTSTRAP, true);
    }

    @Override
    public boolean isCreateNewTokenEnable() {
        return config.get(CONFIG_CREATE_NEW_TOKEN_ENABLE, true);
    }

    @Override
    public String getPrivateKeyLocation() {
        return config.get(CONFIG_PRIKEY_LOC);
    }

    @Override
    public String getRestoreSourceType() {
        return config.get(CONFIG_RESTORE_SOURCE_TYPE);
    }

    @Override
    public boolean isEncryptBackupEnabled() {
        return config.get(CONFIG_ENCRYPTED_BACKUP_ENABLED, false);
    }

    @Override
    public String getAWSRoleAssumptionArn() {
        return config.get(CONFIG_S3_ROLE_ASSUMPTION_ARN);
    }

    @Override
    public String getClassicEC2RoleAssumptionArn() {
        return config.get(CONFIG_EC2_ROLE_ASSUMPTION_ARN);
    }

    @Override
    public String getVpcEC2RoleAssumptionArn() {
        return config.get(CONFIG_VPC_ROLE_ASSUMPTION_ARN);
    }

    @Override
    public boolean isDualAccount() {
        return config.get(CONFIG_DUAL_ACCOUNT, DEFAULT_DUAL_ACCOUNT);
    }

    @Override
    public String getGcsServiceAccountId() {
        return config.get(CONFIG_GCS_SERVICE_ACCT_ID);
    }

    @Override
    public String getGcsServiceAccountPrivateKeyLoc() {
        return config.get(
                CONFIG_GCS_SERVICE_ACCT_PRIVATE_KEY_LOC, "/apps/tomcat/conf/gcsentryptedkey.p12");
    }

    @Override
    public String getPgpPasswordPhrase() {
        return config.get(CONFIG_PGP_PASSWORD_PHRASE);
    }

    @Override
    public String getPgpPublicKeyLoc() {
        return config.get(CONFIG_PGP_PUB_KEY_LOC);
    }

    @Override
    /*
     * @return the vpc id of the running instance.
     */
    public String getVpcId() {
        return NETWORK_VPC;
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
        return config.get(CONFIG_TOMBSTONE_WARNING_THRESHOLD, DEFAULT_TOMBSTONE_WARNING_THRESHOLD);
    }

    @Override
    public int getTombstoneFailureThreshold() {
        return config.get(CONFIG_TOMBSTONE_FAILURE_THRESHOLD, DEFAULT_TOMBSTONE_FAILURE_THRESHOLD);
    }

    @Override
    public int getStreamingSocketTimeoutInMS() {
        return config.get(
                CONFIG_STREAMING_SOCKET_TIMEOUT_IN_MS, DEFAULT_STREAMING_SOCKET_TIMEOUT_IN_MS);
    }

    @Override
    public String getFlushKeyspaces() {
        return config.get(PRIAM_PRE + ".flush.keyspaces");
    }

    @Override
    public String getFlushInterval() {
        return config.get(PRIAM_PRE + ".flush.interval");
    }

    @Override
    public String getBackupStatusFileLoc() {
        return config.get(
                CONFIG_BACKUP_STATUS_FILE_LOCATION,
                getDataFileLocation() + File.separator + "backup.status");
    }

    @Override
    public boolean useSudo() {
        return config.get(CONFIG_CASS_USE_SUDO, true);
    }

    @Override
    public String getBackupNotificationTopicArn() {
        return config.get(PRIAM_PRE + ".backup.notification.topic.arn", "");
    }

    @Override
    public boolean isPostRestoreHookEnabled() {
        return config.get(CONFIG_POST_RESTORE_HOOK_ENABLED, false);
    }

    @Override
    public String getPostRestoreHook() {
        return config.get(CONFIG_POST_RESTORE_HOOK);
    }

    @Override
    public String getPostRestoreHookHeartbeatFileName() {
        return config.get(
                CONFIG_POST_RESTORE_HOOK_HEARTBEAT_FILENAME,
                getDataFileLocation() + File.separator + "postrestorehook_heartbeat");
    }

    @Override
    public String getPostRestoreHookDoneFileName() {
        return config.get(
                CONFIG_POST_RESTORE_HOOK_DONE_FILENAME,
                getDataFileLocation() + File.separator + "postrestorehook_done");
    }

    @Override
    public int getPostRestoreHookTimeOutInDays() {
        return config.get(CONFIG_POST_RESTORE_HOOK_TIMEOUT_IN_DAYS, 2);
    }

    @Override
    public int getPostRestoreHookHeartBeatTimeoutInMs() {
        return config.get(CONFIG_POST_RESTORE_HOOK_HEARTBEAT_TIMEOUT_MS, 120000);
    }

    @Override
    public int getPostRestoreHookHeartbeatCheckFrequencyInMs() {
        return config.get(CONFIG_POST_RESTORE_HOOK_HEARTBEAT_CHECK_FREQUENCY_MS, 120000);
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
}
