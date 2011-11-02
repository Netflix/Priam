package com.priam.defaultimpl;

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.priam.conf.IConfiguration;
import com.priam.utils.SystemUtils;

@Singleton
public class DefaultConfiguration implements IConfiguration
{
    // Constants to fetch the Configuration from simple DB
    private static final String MY_WEBAPP_NAME = "Priam";

    private static final String CONFIG_BUCKET_NAME = MY_WEBAPP_NAME + ".s3.bucket";
    private static final String CONFIG_BACKUP_HOUR = MY_WEBAPP_NAME + ".backup.hour";
    private static final String CONFIG_S3_BASE_DIR = MY_WEBAPP_NAME + ".s3.base_dir";
    private static final String CONFIG_MAX_HEAP_SIZE = MY_WEBAPP_NAME + ".heap.size.";
    private static final String CONFIG_DATA_LOCATION = MY_WEBAPP_NAME + ".data.location";
    private static final String CONFIG_MR_ENABLE = MY_WEBAPP_NAME + ".multiregion.enable";
    private static final String CONFIG_BACKUP_THREADS = MY_WEBAPP_NAME + ".backup.threads";
    private static final String CONFIG_RESTORE_PREFIX = MY_WEBAPP_NAME + ".restore.prefix";
    private static final String CONFIG_CL_LOCATION = MY_WEBAPP_NAME + ".commitlog.location";
    private static final String CONFIG_JMX_LISTERN_PORT_NAME = MY_WEBAPP_NAME + ".jmx.port";
    private static final String CONFIG_RESTORE_THREADS = MY_WEBAPP_NAME + ".restore.threads";
    private static final String CONFIG_AVAILABILITY_ZONES = MY_WEBAPP_NAME + ".zones.available";
    private static final String CONFIG_SAVE_CACHE_LOCATION = MY_WEBAPP_NAME + ".cache.location";
    private static final String CONFIG_INCR_BK_ENABLE = MY_WEBAPP_NAME + ".backup.incremental.enable";
    private static final String CONFIG_CL_BK_ENABLE = MY_WEBAPP_NAME + ".backup.commitlog.enable";
    private static final String CONFIG_NEW_MAX_HEAP_SIZE = MY_WEBAPP_NAME + ".heap.newgen.size.";
    private static final String CONFIG_THRIFT_LISTERN_PORT_NAME = MY_WEBAPP_NAME + ".thrift.port";
    private static final String CONFIG_CL_BK_LOCATION = MY_WEBAPP_NAME + ".backup.commitlog.location";
    private static final String CONFIG_AUTO_RESTORE_SNAPSHOTNAME = MY_WEBAPP_NAME + ".restore.snapshot";

    // TODO make it configurable
    private final String YAML_LOCATION = "/apps/nfcassandra_server/conf/cassandra.yaml";

    // Initialize the configurations from here...
    private Configuration config;

    // Start Populating the configurations.
    private final String S3_BASE_DIR = config.getString(CONFIG_S3_BASE_DIR);
    private final String BUCKET_NAME = config.getString(CONFIG_BUCKET_NAME);

    // Optional configurations
    private final boolean MULTI_REGION_ENABLED = config.getBoolean(CONFIG_MR_ENABLE, false);;
    private final boolean ENABLE_INCR_BACKUP = config.getBoolean(CONFIG_INCR_BK_ENABLE, true);
    private final boolean ENABLE_COMMITLOG_BACKUP = config.getBoolean(CONFIG_CL_BK_ENABLE, false);
    private final String DATA_LOCATION = config.getString(CONFIG_DATA_LOCATION, "/mnt/data/cassandra070/data");
    private final String COMMIT_LOG_LOCATION = config.getString(CONFIG_CL_LOCATION, "/mnt/data/cassandra070/commitlog");
    private final String COMMIT_LOG_BACKUP_LOCATION = config.getString(CONFIG_CL_BK_LOCATION, "/mnt/data/backup/commitlog");
    private final String CACHE_LOCATION = config.getString(CONFIG_SAVE_CACHE_LOCATION, "/mnt/data/cassandra070/saved_caches");
    private final String ENDPOINT_SNITCH = config.getString(MY_WEBAPP_NAME + ".endpoint_snitch", "org.apache.cassandra.locator.Ec2Snitch");
    private final int JMX_PORT = config.getInt(CONFIG_JMX_LISTERN_PORT_NAME, 7501);
    private final int THRIFT_PORT = config.getInt(CONFIG_THRIFT_LISTERN_PORT_NAME, 7102);

    // Environment specific config gets.
    private final String CLUSTER_NAME = System.getenv("NETFLIX_APP");
    private final String REGION = System.getenv("EC2_REGION");
    private static final String AUTO_SCALE_GROUP_NAME = System.getenv("NETFLIX_AUTO_SCALE_GROUP");
    private final List<String> AVAILABILITY_ZONES = config.getList(CONFIG_AVAILABILITY_ZONES);
    private final String RAC = SystemUtils.apiCall("http://169.254.169.254/latest/meta-data/placement/availability-zone");
    private final String PUBLIC_HOSTNAME = SystemUtils.apiCall("http://169.254.169.254/latest/meta-data/public-hostname");
    private final String PUBLIC_IP = SystemUtils.apiCall("http://169.254.169.254/latest/meta-data/public-ipv4");

    private final String INSTANCE_ID = SystemUtils.apiCall("http://169.254.169.254/latest/meta-data/instance-id");
    private final String INSTANCE_TYPE = SystemUtils.apiCall("http://169.254.169.254/latest/meta-data/instance-type");

    // Appended by the machine type--- Instance specific configuration.
    private final String MAX_HEAP_SIZE = config.getString(CONFIG_MAX_HEAP_SIZE + INSTANCE_TYPE, "12G");
    private final String HEAP_NEWSIZE = config.getString(CONFIG_NEW_MAX_HEAP_SIZE + INSTANCE_TYPE, "2G");

    // Time when backup needs to run. Format (0-23) UTC. For multiple hours, spe
    private final Integer BACKUP_HOUR = config.getInt(CONFIG_BACKUP_HOUR, 12);
    private final Integer BACKUP_THREADS = config.getInt(CONFIG_BACKUP_THREADS, 10);
    private final Integer RESTORE_THREADS = config.getInt(CONFIG_RESTORE_THREADS, 30);
    private final String AUTO_RESTORE_SNAPSHOTNAME = config.getString(CONFIG_AUTO_RESTORE_SNAPSHOTNAME, "");
    private final String RESTORE_PREFIX = config.getString(CONFIG_RESTORE_PREFIX, "");

    @Inject
    public DefaultConfiguration() throws ConfigurationException
    {
        this.config = new PropertiesConfiguration(new File("conf/Priam.properties"));
    }

    public void intialize()
    {
        SystemUtils.createDirs(COMMIT_LOG_BACKUP_LOCATION);
        SystemUtils.createDirs(COMMIT_LOG_LOCATION);
        SystemUtils.createDirs(CACHE_LOCATION);
        SystemUtils.createDirs(DATA_LOCATION);
    }

    @Override
    public String getYamlLocation()
    {
        return YAML_LOCATION;
    }

    @Override
    public String getBackupLocation()
    {
        return S3_BASE_DIR;
    }

    @Override
    public String getBackupPrefix()
    {
        return BUCKET_NAME;
    }

    @Override
    public boolean isCommitLogBackup()
    {
        return ENABLE_COMMITLOG_BACKUP;
    }

    @Override
    public String getCommitLogLocation()
    {
        return COMMIT_LOG_LOCATION;
    }

    @Override
    public String getDataFileLocation()
    {
        return DATA_LOCATION;
    }

    @Override
    public String getCacheLocation()
    {
        return CACHE_LOCATION;
    }

    @Override
    public List<String> getRacs()
    {
        return AVAILABILITY_ZONES;
    }

    @Override
    public int getJmxPort()
    {
        return JMX_PORT;
    }

    @Override
    public int getThriftPort()
    {
        return THRIFT_PORT;
    }

    @Override
    public String getSnitch()
    {
        return ENDPOINT_SNITCH;
    }

    public boolean isMultiDC()
    {
        return MULTI_REGION_ENABLED;
    }

    @Override
    public String getHostname()
    {
        return PUBLIC_HOSTNAME;
    }

    @Override
    public String getInstanceName()
    {
        return INSTANCE_ID;
    }

    @Override
    public String getHeapSize()
    {
        return MAX_HEAP_SIZE;
    }

    @Override
    public String getHeapNewSize()
    {
        return HEAP_NEWSIZE;
    }

    @Override
    public boolean isIncrBackup()
    {
        return ENABLE_INCR_BACKUP;
    }

    @Override
    public int getBackupHour()
    {
        return BACKUP_HOUR;
    }

    @Override
    public String getRestoreSnapshot()
    {
        return AUTO_RESTORE_SNAPSHOTNAME;
    }

    @Override
    public boolean isExperimental()
    {
        return false;
    }

    @Override
    public String getAppName()
    {
        return CLUSTER_NAME;
    }

    @Override
    public String getRac()
    {
        return RAC;
    }

    @Override
    public int getMaxBackupUploadThreads()
    {
        return BACKUP_THREADS;
    }

    @Override
    public String getDC()
    {
        return REGION;
    }

    @Override
    public int getMaxBackupDownloadThreads()
    {
        return RESTORE_THREADS;
    }

    @Override
    public String getRestorePrefix()
    {
        return RESTORE_PREFIX;
    }

    @Override
    public String getBackupCommitLogLocation()
    {
        return COMMIT_LOG_BACKUP_LOCATION;
    }

    @Override
    public String getASGName()
    {
        return AUTO_SCALE_GROUP_NAME;
    }

    @Override
    public String getHostIP()
    {
        return PUBLIC_IP;
    }
    @Override 
    public String getBootClusterName(){
    	return CLUSTER_NAME;	
    }
    @Override
    public String getMaxDirectMemory(){
        return null;
    }
    @Override
    public int getCompactionThroughput(){
        return (1);
    }
    @Override
    public int getInMemoryCompactionLimit(){
        return (1);
    }
    @Override
    public int getUploadThrottle(){
        return (1);
    }
    @Override
    public boolean isLocalBootstrapEnabled(){
        return false;
    }
}
		
