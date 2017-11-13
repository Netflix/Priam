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
package com.netflix.priam.defaultimpl;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfigSource;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.identity.InstanceEnvIdentity;
import com.netflix.priam.identity.config.InstanceDataRetriever;
import com.netflix.priam.scheduler.SchedulerType;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final String CONFIG_JMX_ENABLE_REMOTE = PRIAM_PRE + ".jmx.remote.enable";
    private static final String CONFIG_AVAILABILITY_ZONES = PRIAM_PRE + ".zones.available";
    private static final String CONFIG_SAVE_CACHE_LOCATION = PRIAM_PRE + ".cache.location";
    private static final String CONFIG_NEW_MAX_HEAP_SIZE = PRIAM_PRE + ".heap.newgen.size.";
    private static final String CONFIG_DIRECT_MAX_HEAP_SIZE = PRIAM_PRE + ".direct.memory.size.";
    private static final String CONFIG_THRIFT_LISTEN_PORT_NAME = PRIAM_PRE + ".thrift.port";
    private static final String CONFIG_THRIFT_ENABLED = PRIAM_PRE + ".thrift.enabled";
    private static final String CONFIG_NATIVE_PROTOCOL_PORT = PRIAM_PRE + ".nativeTransport.port";
    private static final String CONFIG_NATIVE_PROTOCOL_ENABLED = PRIAM_PRE + ".nativeTransport.enabled";
    private static final String CONFIG_STORAGE_LISTERN_PORT_NAME = PRIAM_PRE + ".storage.port";
    private static final String CONFIG_SSL_STORAGE_LISTERN_PORT_NAME = PRIAM_PRE + ".ssl.storage.port";
    private static final String CONFIG_CL_BK_LOCATION = PRIAM_PRE + ".backup.commitlog.location";
    private static final String CONFIG_THROTTLE_UPLOAD_PER_SECOND = PRIAM_PRE + ".upload.throttle";
    private static final String CONFIG_IN_MEMORY_COMPACTION_LIMIT = PRIAM_PRE + ".memory.compaction.limit";
    private static final String CONFIG_COMPACTION_THROUHPUT = PRIAM_PRE + ".compaction.throughput";
    private static final String CONFIG_MAX_HINT_WINDOW_IN_MS = PRIAM_PRE + ".hint.window";
    private static final String CONFIG_HINT_DELAY = PRIAM_PRE + ".hint.delay";
    private static final String CONFIG_BOOTCLUSTER_NAME = PRIAM_PRE + ".bootcluster";
    private static final String CONFIG_ENDPOINT_SNITCH = PRIAM_PRE + ".endpoint_snitch";
    private static final String CONFIG_MEMTABLE_TOTAL_SPACE = PRIAM_PRE + ".memtabletotalspace";
    private static final String CONFIG_MEMTABLE_CLEANUP_THRESHOLD = PRIAM_PRE + ".memtable.cleanup.threshold";
    private static final String CONFIG_CASS_PROCESS_NAME = PRIAM_PRE + ".cass.process";
    private static final String CONFIG_VNODE_NUM_TOKENS = PRIAM_PRE + ".vnodes.numTokens";
    private static final String CONFIG_YAML_LOCATION = PRIAM_PRE + ".yamlLocation";
    private static final String CONFIG_AUTHENTICATOR = PRIAM_PRE + ".authenticator";
    private static final String CONFIG_AUTHORIZER = PRIAM_PRE + ".authorizer";
    private static final String CONFIG_TARGET_KEYSPACE_NAME = PRIAM_PRE + ".target.keyspace";
    private static final String CONFIG_TARGET_COLUMN_FAMILY_NAME = PRIAM_PRE + ".target.columnfamily";
    private static final String CONFIG_CASS_MANUAL_START_ENABLE = PRIAM_PRE + ".cass.manual.start.enable";
    private static final String CONFIG_CREATE_NEW_TOKEN_ENABLE = PRIAM_PRE + ".create.new.token.enable";

    // Backup and Restore
    private static final String CONFIG_BACKUP_THREADS = PRIAM_PRE + ".backup.threads";
    private static final String CONFIG_RESTORE_PREFIX = PRIAM_PRE + ".restore.prefix";
    private static final String CONFIG_INCR_BK_ENABLE = PRIAM_PRE + ".backup.incremental.enable";
    private static final String CONFIG_SNAPSHOT_KEYSPACE_FILTER = PRIAM_PRE + ".snapshot.keyspace.filter";
    private static final String CONFIG_SNAPSHOT_CF_FILTER = PRIAM_PRE + ".snapshot.cf.filter";
    private static final String CONFIG_INCREMENTAL_KEYSPACE_FILTER = PRIAM_PRE + ".incremental.keyspace.filter";
    private static final String CONFIG_INCREMENTAL_CF_FILTER = PRIAM_PRE + ".incremental.cf.filter";
    private static final String CONFIG_RESTORE_KEYSPACE_FILTER = PRIAM_PRE + ".restore.keyspace.filter";
    private static final String CONFIG_RESTORE_CF_FILTER = PRIAM_PRE + ".restore.cf.filter";

    private static final String CONFIG_CL_BK_ENABLE = PRIAM_PRE + ".backup.commitlog.enable";
    private static final String CONFIG_AUTO_RESTORE_SNAPSHOTNAME = PRIAM_PRE + ".restore.snapshot";
    private static final String CONFIG_BUCKET_NAME = PRIAM_PRE + ".s3.bucket";
    private static final String CONFIG_BACKUP_SCHEDULE_TYPE = PRIAM_PRE + ".backup.schedule.type";
    private static final String CONFIG_BACKUP_HOUR = PRIAM_PRE + ".backup.hour";
    private static final String CONFIG_BACKUP_CRON_EXPRESSION = PRIAM_PRE + ".backup.cron";
    private static final String CONFIG_S3_BASE_DIR = PRIAM_PRE + ".s3.base_dir";
    private static final String CONFIG_RESTORE_THREADS = PRIAM_PRE + ".restore.threads";
    private static final String CONFIG_RESTORE_CLOSEST_TOKEN = PRIAM_PRE + ".restore.closesttoken";
    private static final String CONFIG_RESTORE_KEYSPACES = PRIAM_PRE + ".restore.keyspaces";
    private static final String CONFIG_BACKUP_CHUNK_SIZE = PRIAM_PRE + ".backup.chunksizemb";
    private static final String CONFIG_BACKUP_RETENTION = PRIAM_PRE + ".backup.retention";
    private static final String CONFIG_BACKUP_RACS = PRIAM_PRE + ".backup.racs";
    private static final String CONFIG_MULTITHREADED_COMPACTION = PRIAM_PRE + ".multithreaded.compaction";
    private static final String CONFIG_STREAMING_THROUGHPUT_MB = PRIAM_PRE + ".streaming.throughput.mb";
    private static final String CONFIG_STREAMING_SOCKET_TIMEOUT_IN_MS = PRIAM_PRE + ".streaming.socket.timeout.ms";
    private static final String CONFIG_TOMBSTONE_FAILURE_THRESHOLD = PRIAM_PRE + ".tombstone.failure.threshold";
    private static final String CONFIG_TOMBSTONE_WARNING_THRESHOLD = PRIAM_PRE + ".tombstone.warning.threshold";

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
    private static final String CONFIG_COMMITLOG_RESTORE_POINT_IN_TIME = PRIAM_PRE + ".clbackup.restoreTime";
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
    private static final String CONFIG_INDEX_INTERVAL = PRIAM_PRE + ".index.interval";
    private static final String CONFIG_EXTRA_PARAMS = PRIAM_PRE + ".extra.params";
    private static final String CONFIG_AUTO_BOOTSTRAP = PRIAM_PRE + ".auto.bootstrap";
    private static final String CONFIG_DSE_CLUSTER_TYPE = PRIAM_PRE + ".dse.cluster.type";
    private static final String CONFIG_EXTRA_ENV_PARAMS = PRIAM_PRE + ".extra.env.params";

    private static final String CONFIG_RESTORE_SOURCE_TYPE = PRIAM_PRE + ".restore.source.type"; //the type of source for the restore.  Valid values are: AWSCROSSACCT or GOOGLE.
    private static final String CONFIG_ENCRYPTED_BACKUP_ENABLED = PRIAM_PRE + ".encrypted.backup.enabled"; //enable encryption of backup (snapshots, incrementals, commit logs).

    //Backup and restore cryptography
    private static final String CONFIG_PRIKEY_LOC = PRIAM_PRE + ".private.key.location"; //the location on disk of the private key used by the cryptography algorithm
    private static final String CONFIG_PGP_PASSWORD_PHRASE = PRIAM_PRE + ".pgp.password.phrase"; //pass phrase used by the cryptography algorithm
    private static final String CONFIG_PGP_PUB_KEY_LOC = PRIAM_PRE + ".pgp.pubkey.file.location";

    //Restore from Google Cloud Storage
    private static final String CONFIG_GCS_SERVICE_ACCT_ID = PRIAM_PRE + ".gcs.service.acct.id"; //Google Cloud Storage service account id
    private static final String CONFIG_GCS_SERVICE_ACCT_PRIVATE_KEY_LOC = PRIAM_PRE + ".gcs.service.acct.private.key"; //the absolute path on disk for the Google Cloud Storage PFX file (i.e. the combined format of the private key and certificate).

    // Amazon specific
    private static final String CONFIG_ASG_NAME = PRIAM_PRE + ".az.asgname";
    private static final String CONFIG_SIBLING_ASG_NAMES = PRIAM_PRE + ".az.sibling.asgnames";
    private static final String CONFIG_REGION_NAME = PRIAM_PRE + ".az.region";
    private static final String CONFIG_ACL_GROUP_NAME = PRIAM_PRE + ".acl.groupname";
    private final String LOCAL_HOSTNAME = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-hostname").trim();
    private final String LOCAL_IP = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-ipv4").trim();
    private static String ASG_NAME = System.getenv("ASG_NAME");
    private static String REGION = System.getenv("EC2_REGION");
    private static final String CONFIG_VPC_RING = PRIAM_PRE + ".vpc";
    private static final String CONFIG_S3_ROLE_ASSUMPTION_ARN = PRIAM_PRE + ".roleassumption.arn"; //Restore from AWS.  This is applicable when restoring from an AWS account which requires cross account assumption. 
    private static final String CONFIG_EC2_ROLE_ASSUMPTION_ARN = PRIAM_PRE + ".ec2.roleassumption.arn";
    private static final String CONFIG_VPC_ROLE_ASSUMPTION_ARN = PRIAM_PRE + ".vpc.roleassumption.arn";
    private static final String CONFIG_DUAL_ACCOUNT = PRIAM_PRE + ".roleassumption.dualaccount";


    //Running instance meta data
    private String RAC;
    private String PUBLIC_HOSTNAME;
    private String PUBLIC_IP;
    private String INSTANCE_TYPE;
    private String INSTANCE_ID;
    private String NETWORK_MAC;  //Fetch metadata of the running instance's network interface

    //== vpc specific   
    private String NETWORK_VPC;  //Fetch the vpc id of running instance

    // Defaults 
    private final String DEFAULT_CLUSTER_NAME = "cass_cluster";
    private final String DEFAULT_DATA_LOCATION = "/var/lib/cassandra/data";
    private final String DEFAULT_LOGS_LOCATION = "/var/lib/cassandra/logs";
    private final String DEFAULT_COMMIT_LOG_LOCATION = "/var/lib/cassandra/commitlog";
    private final String DEFAULT_CACHE_LOCATION = "/var/lib/cassandra/saved_caches";
    private final String DEFAULT_ENDPOINT_SNITCH = "org.apache.cassandra.locator.Ec2Snitch";
    private final String DEFAULT_SEED_PROVIDER = "com.netflix.priam.cassandra.extensions.NFSeedProvider";
    private final String DEFAULT_PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
    public static final String DEFAULT_AUTHENTICATOR = "org.apache.cassandra.auth.AllowAllAuthenticator";
    public static final String DEFAULT_AUTHORIZER = "org.apache.cassandra.auth.AllowAllAuthorizer";
    public static final String DEFAULT_COMMITLOG_PROPS_FILE = "/conf/commitlog_archiving.properties";

    // rpm based. Can be modified for tar based.
    private final String DEFAULT_CASS_HOME_DIR = "/etc/cassandra";
    private final String DEFAULT_CASS_START_SCRIPT = "/etc/init.d/cassandra start";
    private final String DEFAULT_CASS_STOP_SCRIPT = "/etc/init.d/cassandra stop";
    private final String DEFAULT_BACKUP_LOCATION = "backup";
    private final String DEFAULT_BUCKET_NAME = "cassandra-archive";
    //    private String DEFAULT_AVAILABILITY_ZONES = "";
    private List<String> DEFAULT_AVAILABILITY_ZONES = ImmutableList.of();
    private final String DEFAULT_CASS_PROCESS_NAME = "CassandraDaemon";

    private final String DEFAULT_MAX_DIRECT_MEM = "50G";
    private final String DEFAULT_MAX_HEAP = "8G";
    private final String DEFAULT_MAX_NEWGEN_HEAP = "2G";
    private final int DEFAULT_JMX_PORT = 7199;
    private final int DEFAULT_THRIFT_PORT = 9160;
    private final int DEFAULT_NATIVE_PROTOCOL_PORT = 9042;
    private final int DEFAULT_STORAGE_PORT = 7000;
    private final int DEFAULT_SSL_STORAGE_PORT = 7001;
    private final int DEFAULT_BACKUP_HOUR = 12;
    private final String DEFAULT_BACKUP_CRON_EXPRESSION = "0 0 12 1/1 * ? *"; //Backup daily at 12.
    private final int DEFAULT_BACKUP_THREADS = 2;
    private final int DEFAULT_RESTORE_THREADS = 8;
    private final int DEFAULT_BACKUP_CHUNK_SIZE = 10;
    private final int DEFAULT_BACKUP_RETENTION = 0;
    private final int DEFAULT_VNODE_NUM_TOKENS = 1;
    private final int DEFAULT_HINTS_MAX_THREADS = 2; //default value from 1.2 yaml
    private final int DEFAULT_HINTS_THROTTLE_KB = 1024; //default value from 1.2 yaml
    private final String DEFAULT_INTERNODE_COMPRESSION = "all";  //default value from 1.2 yaml

    private static final String DEFAULT_RPC_SERVER_TYPE = "hsha";
    private static final int DEFAULT_RPC_MIN_THREADS = 16;
    private static final int DEFAULT_RPC_MAX_THREADS = 2048;
    private static final int DEFAULT_INDEX_INTERVAL = 256;
    private static final int DEFAULT_STREAMING_SOCKET_TIMEOUT_IN_MS = 86400000; // 24 Hours
    private static final int DEFAULT_TOMBSTONE_WARNING_THRESHOLD = 1000; // C* defaults
    private static final int DEFAULT_TOMBSTONE_FAILURE_THRESHOLD = 100000;// C* defaults

    // AWS EC2 Dual Account
    private static final boolean DEFAULT_DUAL_ACCOUNT = false;

    private final IConfigSource config;
    private final String BLANK = "";
    private static final Logger logger = LoggerFactory.getLogger(PriamConfiguration.class);
    private final ICredential provider;

    private InstanceEnvIdentity insEnvIdentity;

    @Inject
    public PriamConfiguration(ICredential provider, IConfigSource config, InstanceEnvIdentity insEnvIdentity) {
        // public interface meta-data does not exist when Priam runs in AWS VPC (priam.vpc=true)
        String p_hostname = "";
        String p_ip = "";
        try {
            p_hostname = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-hostname").trim();
        } catch (RuntimeException ex) {
            // swallow
        }
        try {
            p_ip = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-ipv4").trim();
        } catch (RuntimeException ex) {
            // swallow
        }
        this.PUBLIC_HOSTNAME = p_hostname;
        this.PUBLIC_IP = p_ip;
        this.provider = provider;
        this.config = config;
        this.insEnvIdentity = insEnvIdentity;
    }

    @Override
    public void intialize() {
        InstanceDataRetriever instanceDataRetriever;
        try {
            instanceDataRetriever = getInstanceDataRetriever();
        } catch (Exception e) {
            throw new IllegalStateException("Exception when instantiating the instance data retriever.  Msg: " + e.getLocalizedMessage());
        }

        RAC = instanceDataRetriever.getRac();
        PUBLIC_HOSTNAME = instanceDataRetriever.getPublicHostname();
        PUBLIC_IP = instanceDataRetriever.getPublicIP();

        INSTANCE_ID = instanceDataRetriever.getInstanceId();
        INSTANCE_TYPE = instanceDataRetriever.getInstanceType();

        NETWORK_MAC = instanceDataRetriever.getMac();
        NETWORK_VPC = instanceDataRetriever.getVpcId();

        setupEnvVars();
        this.config.intialize(ASG_NAME, REGION);
        setDefaultRACList(REGION);
        populateProps();
        SystemUtils.createDirs(getBackupCommitLogLocation());
        SystemUtils.createDirs(getCommitLogLocation());
        SystemUtils.createDirs(getCacheLocation());
        SystemUtils.createDirs(getDataFileLocation());
    }

    private InstanceDataRetriever getInstanceDataRetriever() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        if (this.insEnvIdentity.isClassic()) {
            return (InstanceDataRetriever) Class.forName("com.netflix.priam.identity.config.AwsClassicInstanceDataRetriever").newInstance();

        } else if (this.insEnvIdentity.isNonDefaultVpc()) {
            return (InstanceDataRetriever) Class.forName("com.netflix.priam.identity.config.AWSVpcInstanceDataRetriever").newInstance();
        } else {
            throw new IllegalStateException("Unable to determine environemt (vpc, classic) for running instance.");
        }
    }

    private void setupEnvVars() {
        // Search in java opt properties
        REGION = StringUtils.isBlank(REGION) ? System.getProperty("EC2_REGION") : REGION;
        // Infer from zone
        if (StringUtils.isBlank(REGION))
            REGION = RAC.substring(0, RAC.length() - 1);
        ASG_NAME = StringUtils.isBlank(ASG_NAME) ? System.getProperty("ASG_NAME") : ASG_NAME;
        if (StringUtils.isBlank(ASG_NAME))
            ASG_NAME = populateASGName(REGION, INSTANCE_ID);
        logger.info("REGION set to {}, ASG Name set to {}", REGION, ASG_NAME);
    }

    /**
     * Query amazon to get ASG name. Currently not available as part of instance
     * info api.
     */
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
            client = new AmazonEC2Client(provider.getAwsCredentialProvider());
            client.setEndpoint("ec2." + region + ".amazonaws.com");
        }

        @Override
        public String retriableCall() throws IllegalStateException {
            DescribeInstancesRequest desc = new DescribeInstancesRequest().withInstanceIds(instanceId);
            DescribeInstancesResult res = client.describeInstances(desc);

            for (Reservation resr : res.getReservations()) {
                for (Instance ins : resr.getInstances()) {
                    for (com.amazonaws.services.ec2.model.Tag tag : ins.getTags()) {
                        if (tag.getKey().equals("aws:autoscaling:groupName"))
                            return tag.getValue();
                    }
                }
            }

            logger.warn("Couldn't determine ASG name");
            throw new IllegalStateException("Couldn't determine ASG name");
        }
    }

    /**
     * Get the fist 3 available zones in the region
     */
    public void setDefaultRACList(String region) {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + region + ".amazonaws.com");
        DescribeAvailabilityZonesResult res = client.describeAvailabilityZones();
        List<String> zone = Lists.newArrayList();
        for (AvailabilityZone reg : res.getAvailabilityZones()) {
            if (reg.getState().equals("available"))
                zone.add(reg.getZoneName());
            if (zone.size() == 3)
                break;
        }
        DEFAULT_AVAILABILITY_ZONES = ImmutableList.copyOf(zone);
    }

    private void populateProps() {
        config.set(CONFIG_ASG_NAME, ASG_NAME);
        config.set(CONFIG_REGION_NAME, REGION);
    }

    @Override
    public String getCassStartupScript() {
        return config.get(CONFIG_CASS_START_SCRIPT, DEFAULT_CASS_START_SCRIPT);
    }

    @Override
    public String getCassStopScript() {
        return config.get(CONFIG_CASS_STOP_SCRIPT, DEFAULT_CASS_STOP_SCRIPT);
    }

    @Override
    public String getCassHome() {
        return config.get(CONFIG_CASS_HOME_DIR, DEFAULT_CASS_HOME_DIR);
    }

    @Override
    public String getBackupLocation() {
        return config.get(CONFIG_S3_BASE_DIR, DEFAULT_BACKUP_LOCATION);
    }

    @Override
    public String getBackupPrefix() {
        return config.get(CONFIG_BUCKET_NAME, DEFAULT_BUCKET_NAME);
    }

    @Override
    public int getBackupRetentionDays() {
        return config.get(CONFIG_BACKUP_RETENTION, DEFAULT_BACKUP_RETENTION);
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
    public List<String> getRestoreKeySpaces() {
        return config.getList(CONFIG_RESTORE_KEYSPACES);
    }

    @Override
    public String getDataFileLocation() {
        return config.get(CONFIG_DATA_LOCATION, DEFAULT_DATA_LOCATION);
    }

    @Override
    public String getLogDirLocation()
    {
        return config.get(CONFIG_LOGS_LOCATION, DEFAULT_LOGS_LOCATION);
    }

    @Override
    public String getCacheLocation()
    {
        return config.get(CONFIG_SAVE_CACHE_LOCATION, DEFAULT_CACHE_LOCATION);
    }

    @Override
    public String getCommitLogLocation() {
        return config.get(CONFIG_CL_LOCATION, DEFAULT_COMMIT_LOG_LOCATION);
    }

    @Override
    public String getBackupCommitLogLocation() {
        return config.get(CONFIG_CL_BK_LOCATION, "");
    }

    @Override
    public long getBackupChunkSize() {
        long size = config.get(CONFIG_BACKUP_CHUNK_SIZE, DEFAULT_BACKUP_CHUNK_SIZE);
        return size * 1024 * 1024L;
    }

    @Override
    public int getJmxPort() {
        return config.get(CONFIG_JMX_LISTERN_PORT_NAME, DEFAULT_JMX_PORT);
    }

    /**
     * @return Enables Remote JMX connections n C*
     */
    @Override
    public boolean enableRemoteJMX() {
        return config.get(CONFIG_JMX_ENABLE_REMOTE, false);
    }

    public int getNativeTransportPort() {
        return config.get(CONFIG_NATIVE_PROTOCOL_PORT, DEFAULT_NATIVE_PROTOCOL_PORT);
    }

    @Override
    public int getThriftPort() {
        return config.get(CONFIG_THRIFT_LISTEN_PORT_NAME, DEFAULT_THRIFT_PORT);
    }

    @Override
    public int getStoragePort() {
        return config.get(CONFIG_STORAGE_LISTERN_PORT_NAME, DEFAULT_STORAGE_PORT);
    }

    @Override
    public int getSSLStoragePort() {
        return config.get(CONFIG_SSL_STORAGE_LISTERN_PORT_NAME, DEFAULT_SSL_STORAGE_PORT);
    }

    @Override
    public String getSnitch() {
        return config.get(CONFIG_ENDPOINT_SNITCH, DEFAULT_ENDPOINT_SNITCH);
    }

    @Override
    public String getAppName() {
        return config.get(CONFIG_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    }

    @Override
    public String getRac() {
        return RAC;
    }

    @Override
    public List<String> getRacs() {
        return config.getList(CONFIG_AVAILABILITY_ZONES, DEFAULT_AVAILABILITY_ZONES);
    }

    @Override
    public String getHostname() {
        if (this.isVpcRing()) return LOCAL_IP;
        else return PUBLIC_HOSTNAME;
    }

    @Override
    public String getInstanceName() {
        return INSTANCE_ID;
    }

    @Override
    public String getHeapSize() {
        return config.get(CONFIG_MAX_HEAP_SIZE + INSTANCE_TYPE, DEFAULT_MAX_HEAP);
    }

    @Override
    public String getHeapNewSize() {
        return config.get(CONFIG_NEW_MAX_HEAP_SIZE + INSTANCE_TYPE, DEFAULT_MAX_NEWGEN_HEAP);
    }

    @Override
    public String getMaxDirectMemory() {
        return config.get(CONFIG_DIRECT_MAX_HEAP_SIZE + INSTANCE_TYPE, DEFAULT_MAX_DIRECT_MEM);
    }

    @Override
    public int getBackupHour() {
        return config.get(CONFIG_BACKUP_HOUR, DEFAULT_BACKUP_HOUR);
    }

    @Override
    public String getBackupCronExpression() {
        return config.get(CONFIG_BACKUP_CRON_EXPRESSION, DEFAULT_BACKUP_CRON_EXPRESSION);
    }

    @Override
    public SchedulerType getBackupSchedulerType() throws UnsupportedTypeException {
        String schedulerType = config.get(CONFIG_BACKUP_SCHEDULE_TYPE, SchedulerType.HOUR.getSchedulerType());
        return SchedulerType.lookup(schedulerType);
    }

    @Override
    public SchedulerType getFlushSchedulerType() throws UnsupportedTypeException {
        String schedulerType = config.get(PRIAM_PRE + ".flush.schedule.type", SchedulerType.HOUR.getSchedulerType());
        return SchedulerType.lookup(schedulerType);
    }

    @Override
    public String getFlushCronExpression() {
        return config.get(PRIAM_PRE + ".flush.cron");
    }

    @Override
    public String getSnapshotKeyspaceFilters() {
        return config.get(CONFIG_SNAPSHOT_KEYSPACE_FILTER);
    }

    @Override
    public String getSnapshotCFFilter() throws IllegalArgumentException {
        return config.get(CONFIG_SNAPSHOT_CF_FILTER);
    }

    @Override
    public String getIncrementalKeyspaceFilters() {
        return config.get(CONFIG_INCREMENTAL_KEYSPACE_FILTER);
    }

    @Override
    public String getIncrementalCFFilter() {
        return config.get(CONFIG_INCREMENTAL_CF_FILTER);
    }

    @Override
    public String getRestoreKeyspaceFilter() {
        return config.get(CONFIG_RESTORE_KEYSPACE_FILTER);
    }

    @Override
    public String getRestoreCFFilter() {
        return config.get(CONFIG_RESTORE_CF_FILTER);
    }

    @Override
    public String getRestoreSnapshot() {
        return config.get(CONFIG_AUTO_RESTORE_SNAPSHOTNAME, "");
    }

    @Override
    public boolean isRestoreEncrypted(){
        return config.get(PRIAM_PRE + ".encrypted.restore.enabled", false);
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
    public int getMaxBackupUploadThreads() {

        return config.get(CONFIG_BACKUP_THREADS, DEFAULT_BACKUP_THREADS);
    }

    @Override
    public int getMaxBackupDownloadThreads() {
        return config.get(CONFIG_RESTORE_THREADS, DEFAULT_RESTORE_THREADS);
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
     * Amazon specific setting to query Additional/ Sibling ASG Memberships in csv format to consider while calculating RAC membership
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
        if (this.isVpcRing()) return LOCAL_IP;
        else return PUBLIC_IP;
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
    public int getInMemoryCompactionLimit() {
        return config.get(CONFIG_IN_MEMORY_COMPACTION_LIMIT, 128);
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
        return config.get(CONFIG_HINTS_THROTTLE_KB, DEFAULT_HINTS_THROTTLE_KB);
    }

    public int getMaxHintThreads() {
        return config.get(CONFIG_MAX_HINT_THREADS, DEFAULT_HINTS_MAX_THREADS);
    }

    @Override
    public String getBootClusterName() {
        return config.get(CONFIG_BOOTCLUSTER_NAME, "");
    }

    @Override
    public String getSeedProviderName() {
        return config.get(CONFIG_SEED_PROVIDER_NAME, DEFAULT_SEED_PROVIDER);
    }

    @Override
    /**
     * Defaults to 0, means dont set it in yaml
     */
    public int getMemtableTotalSpaceMB() {
        return config.get(CONFIG_MEMTABLE_TOTAL_SPACE, 1024);
    }

    /**
     * memtable_cleanup_threshold defaults to 1 / (memtable_flush_writers + 1) = 0.11
     */
    public double getMemtableCleanupThreshold() {
        return config.get(CONFIG_MEMTABLE_CLEANUP_THRESHOLD, 0.11);
    }

    @Override
    public int getStreamingThroughputMB() {
        return config.get(CONFIG_STREAMING_THROUGHPUT_MB, 400);
    }

    @Override
    public boolean getMultithreadedCompaction() {
        return config.get(CONFIG_MULTITHREADED_COMPACTION, false);
    }

    public String getPartitioner() {
        return config.get(CONFIG_PARTITIONER, DEFAULT_PARTITIONER);
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
        return config.get(CONFIG_CASS_PROCESS_NAME, DEFAULT_CASS_PROCESS_NAME);
    }

    public int getNumTokens() {
        return config.get(CONFIG_VNODE_NUM_TOKENS, DEFAULT_VNODE_NUM_TOKENS);
    }

    public String getYamlLocation() {
        return config.get(CONFIG_YAML_LOCATION, getCassHome() + "/conf/cassandra.yaml");
    }

    public String getAuthenticator() {
        return config.get(CONFIG_AUTHENTICATOR, DEFAULT_AUTHENTICATOR);
    }

    public String getAuthorizer() {
        return config.get(CONFIG_AUTHORIZER, DEFAULT_AUTHORIZER);
    }

    public String getTargetKSName() {
        return config.get(CONFIG_TARGET_KEYSPACE_NAME);
    }

    @Override
    public String getTargetCFName() {
        return config.get(CONFIG_TARGET_COLUMN_FAMILY_NAME);
    }

    @Override
    public boolean doesCassandraStartManually() {
        return config.get(CONFIG_CASS_MANUAL_START_ENABLE, false);
    }

    public String getInternodeCompression() {
        return config.get(CONFIG_INTERNODE_COMPRESSION, DEFAULT_INTERNODE_COMPRESSION);
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
        return config.get(CONFIG_COMMITLOG_PROPS_FILE, getCassHome() + DEFAULT_COMMITLOG_PROPS_FILE);
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

    @Override
    public void setRestoreKeySpaces(List<String> keyspaces) {
        if (keyspaces == null)
            return;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyspaces.size(); i++) {
            if (i > 0)
                sb.append(",");

            sb.append(keyspaces.get(i));
        }

        config.set(CONFIG_RESTORE_KEYSPACES, sb.toString());
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

    public int getIndexInterval() {
        return config.get(CONFIG_INDEX_INTERVAL, DEFAULT_INDEX_INTERVAL);
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
        Map<String, String> extraEnvParamsMap = new HashMap<String, String>();
        String[] pairs = envParams.split(",");
        logger.info("getExtraEnvParams: Extra cass params. From config :{}", envParams);
        for (int i = 0; i < pairs.length; i++) {
            String[] pair = pairs[i].split("=");
            if (pair.length > 1) {
                String priamKey = pair[0];
                String cassKey = pair[1];
                String cassVal = config.get(priamKey);
                logger.info("getExtraEnvParams: Start-up/ env params: Priamkey[{}], CassStartupKey[{}], Val[{}]", priamKey, cassKey, cassVal);
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

    //values are cassandra, solr, hadoop, spark or hadoop-spark
    public String getDseClusterType() {
        return config.get(CONFIG_DSE_CLUSTER_TYPE + "." + ASG_NAME, "cassandra");
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
        return config.get(CONFIG_GCS_SERVICE_ACCT_PRIVATE_KEY_LOC, "/apps/tomcat/conf/gcsentryptedkey.p12");
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
    public Boolean isIncrBackupParallelEnabled() {
        return config.get(PRIAM_PRE + ".incremental.bkup.parallel", false);
    }

    @Override
    public int getIncrementalBkupMaxConsumers() {
        return config.get(PRIAM_PRE + ".incremental.bkup.max.consumers", 4);
    }

    @Override
    public int getUncrementalBkupQueueSize() {
        return config.get(PRIAM_PRE + ".incremental.bkup.queue.size", 100000);
    }

    /**
     * @return tombstone_warn_threshold in yaml
     */
    @Override
    public int getTombstoneWarnThreshold() {
        return config.get(CONFIG_TOMBSTONE_WARNING_THRESHOLD, DEFAULT_TOMBSTONE_WARNING_THRESHOLD);
    }

    /**
     * @return tombstone_failure_threshold in yaml
     */
    @Override
    public int getTombstoneFailureThreshold() {
        return config.get(CONFIG_TOMBSTONE_FAILURE_THRESHOLD, DEFAULT_TOMBSTONE_FAILURE_THRESHOLD);
    }

    /**
     * @return streaming_socket_timeout_in_ms in yaml
     */
    @Override
    public int getStreamingSocketTimeoutInMS() {
        return config.get(CONFIG_STREAMING_SOCKET_TIMEOUT_IN_MS, DEFAULT_STREAMING_SOCKET_TIMEOUT_IN_MS);
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
        return "backup.status";
    }


    @Override
    public boolean useSudo() {
        return config.get(CONFIG_CASS_USE_SUDO, true);
    }

    @Override
    public String getBackupNotificationTopicArn() {
        return config.get(PRIAM_PRE + ".backup.notification.topic.arn", "");
    }

}
