package com.netflix.priam.defaultimpl;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.io.util.FileUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.common.collect.Lists;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.SystemUtils;

@Singleton
public class PriamConfiguration implements IConfiguration
{
    private static String PRIAM_CONFIG_FILE = "conf/priam.properties";
    private static final String MY_WEBAPP_NAME = "Priam";

    private static final String CONFIG_CASS_HOME_DIR = MY_WEBAPP_NAME + ".cass.home";
    private static final String CONFIG_CASS_START_SCRIPT = MY_WEBAPP_NAME + ".cass.startscript";
    private static final String CONFIG_CASS_STOP_SCRIPT = MY_WEBAPP_NAME + ".cass.stopscript";
    private static final String CONFIG_CLUSTER_NAME = MY_WEBAPP_NAME + ".clustername";
    private static final String CONFIG_LOAD_LOCAL_PROPERTIES = MY_WEBAPP_NAME + ".localbootstrap.enable";
    private static final String CONFIG_MAX_HEAP_SIZE = MY_WEBAPP_NAME + ".heap.size.";
    private static final String CONFIG_DATA_LOCATION = MY_WEBAPP_NAME + ".data.location";
    private static final String CONFIG_MR_ENABLE = MY_WEBAPP_NAME + ".multiregion.enable";
    private static final String CONFIG_CL_LOCATION = MY_WEBAPP_NAME + ".commitlog.location";
    private static final String CONFIG_JMX_LISTERN_PORT_NAME = MY_WEBAPP_NAME + ".jmx.port";
    private static final String CONFIG_AVAILABILITY_ZONES = MY_WEBAPP_NAME + ".zones.available";
    private static final String CONFIG_SAVE_CACHE_LOCATION = MY_WEBAPP_NAME + ".cache.location";
    private static final String CONFIG_NEW_MAX_HEAP_SIZE = MY_WEBAPP_NAME + ".heap.newgen.size.";
    private static final String CONFIG_DIRECT_MAX_HEAP_SIZE = MY_WEBAPP_NAME + ".direct.memory.size.";
    private static final String CONFIG_THRIFT_LISTERN_PORT_NAME = MY_WEBAPP_NAME + ".thrift.port";
    private static final String CONFIG_STORAGE_LISTERN_PORT_NAME = MY_WEBAPP_NAME + ".storage.port";
    private static final String CONFIG_CL_BK_LOCATION = MY_WEBAPP_NAME + ".backup.commitlog.location";
    private static final String CONFIG_THROTTLE_UPLOAD_PER_SECOND = MY_WEBAPP_NAME + ".upload.throttle";
    private static final String CONFIG_IN_MEMORY_COMPACTION_LIMIT = MY_WEBAPP_NAME + ".memory.compaction.limit";
    private static final String CONFIG_COMPACTION_THROUHPUT = MY_WEBAPP_NAME + ".compaction.throughput";
    private static final String CONFIG_BOOTCLUSTER_NAME = MY_WEBAPP_NAME + ".bootcluster";
    private static final String CONFIG_ENDPOINT_SNITCH = MY_WEBAPP_NAME + ".endpoint_snitch";

    // Backup and Restore
    private static final String CONFIG_BACKUP_THREADS = MY_WEBAPP_NAME + ".backup.threads";
    private static final String CONFIG_RESTORE_PREFIX = MY_WEBAPP_NAME + ".restore.prefix";
    private static final String CONFIG_INCR_BK_ENABLE = MY_WEBAPP_NAME + ".backup.incremental.enable";
    private static final String CONFIG_CL_BK_ENABLE = MY_WEBAPP_NAME + ".backup.commitlog.enable";
    private static final String CONFIG_AUTO_RESTORE_SNAPSHOTNAME = MY_WEBAPP_NAME + ".restore.snapshot";
    private static final String CONFIG_BUCKET_NAME = MY_WEBAPP_NAME + ".s3.bucket";
    private static final String CONFIG_BACKUP_HOUR = MY_WEBAPP_NAME + ".backup.hour";
    private static final String CONFIG_S3_BASE_DIR = MY_WEBAPP_NAME + ".s3.base_dir";
    private static final String CONFIG_RESTORE_THREADS = MY_WEBAPP_NAME + ".restore.threads";
    private static final String CONFIG_RESTORE_CLOSEST_TOKEN = MY_WEBAPP_NAME + ".restore.closesttoken";
    private static final String CONFIG_RESTORE_KEYSPACES = MY_WEBAPP_NAME + ".restore.keyspaces";
    private static final String CONFIG_BACKUP_CHUNK_SIZE = MY_WEBAPP_NAME + ".backup.chunksizemb";

    // Amazon specific
    private static final String CONFIG_ASG_NAME = MY_WEBAPP_NAME + ".az.asgname";
    private static final String CONFIG_REGION_NAME = MY_WEBAPP_NAME + ".az.region";
    private final String RAC = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/placement/availability-zone");
    private final String PUBLIC_HOSTNAME = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-hostname");
    private final String PUBLIC_IP = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-ipv4");
    private final String INSTANCE_ID = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-id");
    private final String INSTANCE_TYPE = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-type");

    // Defaults
    private final String DEFAULT_DATA_LOCATION = "/mnt/data/cassandra070/data";
    private final String DEFAULT_COMMIT_LOG_LOCATION = "/mnt/data/cassandra070/commitlog";
    private final String DEFAULT_COMMIT_LOG_BACKUP_LOCATION = "/mnt/data/backup/commitlog";
    private final String DEFAULT_CACHE_LOCATION = "/mnt/data/cassandra070/saved_caches";
    private final String DEFULT_ENDPOINT_SNITCH = "org.apache.cassandra.locator.Ec2Snitch";
    private final int DEFAULT_JMX_PORT = 7199;
    private final int DEFAULT_THRIFT_PORT = 9160;
    private final int DEFAULT_STORAGE_PORT = 7000;
    private final int DEFAULT_BACKUP_HOUR = 12;
    private final int DEFAULT_BACKUP_THREADS = 10;
    private final int DEFAULT_RESTORE_THREADS = 30;
    private final int DEFAULT_BACKUP_CHUNK_SIZE = 10;
    private final int DEFAULT_CL_BACKUP_PORT = 7104;
    private final long DEFAULT_CL_FILE_SIZE = 128L * 1024 * 1024;
    private final int DEFAULT_CL_ROTATE_INTERVAL = 120;

    private PriamProperties config;

    @Inject
    public PriamConfiguration()
    {
        FileInputStream fis = null;
        try
        {
            fis = new FileInputStream(PRIAM_CONFIG_FILE);
            config = new PriamProperties();
            config.load(fis);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Problem reading yaml config. Cannot start.", e);
        }
        finally
        {
            FileUtils.closeQuietly(fis);
        }
    }

    @Override
    public void intialize()
    {
        SystemUtils.createDirs(getBackupCommitLogLocation());
        SystemUtils.createDirs(getCommitLogLocation());
        SystemUtils.createDirs(getCacheLocation());
        SystemUtils.createDirs(getDataFileLocation());
    }

    @Override
    public String getCassStartupScript()
    {
        return config.getProperty(CONFIG_CASS_START_SCRIPT);
    }

    @Override
    public String getCassStopScript()
    {
        return config.getProperty(CONFIG_CASS_STOP_SCRIPT);
    }

    @Override
    public String getCassHome()
    {
        return config.getProperty(CONFIG_CASS_HOME_DIR);
    }

    @Override
    public String getBackupLocation()
    {
        return config.getProperty(CONFIG_S3_BASE_DIR);
    }

    @Override
    public String getBackupPrefix()
    {
        return config.getProperty(CONFIG_BUCKET_NAME);
    }

    @Override
    public String getRestorePrefix()
    {
        return config.getProperty(CONFIG_RESTORE_PREFIX);
    }

    @Override
    public List<String> getRestoreKeySpaces()
    {
        return config.getList(CONFIG_RESTORE_KEYSPACES);
    }

    @Override
    public String getDataFileLocation()
    {
        return config.getProperty(CONFIG_DATA_LOCATION, DEFAULT_DATA_LOCATION);
    }

    @Override
    public String getCacheLocation()
    {
        return config.getProperty(CONFIG_SAVE_CACHE_LOCATION, DEFAULT_CACHE_LOCATION);
    }

    @Override
    public String getCommitLogLocation()
    {
        return config.getProperty(CONFIG_CL_LOCATION, DEFAULT_COMMIT_LOG_LOCATION);
    }

    @Override
    public String getBackupCommitLogLocation()
    {
        return config.getProperty(CONFIG_CL_BK_LOCATION, DEFAULT_COMMIT_LOG_BACKUP_LOCATION);
    }

    @Override
    public long getBackupChunkSize()
    {
        return config.getLong(CONFIG_BACKUP_CHUNK_SIZE, DEFAULT_BACKUP_CHUNK_SIZE);
    }

    @Override
    public boolean isCommitLogBackup()
    {
        return config.getBoolean(CONFIG_CL_BK_ENABLE, false);
    }

    @Override
    public int getJmxPort()
    {
        return config.getInteger(CONFIG_JMX_LISTERN_PORT_NAME, DEFAULT_JMX_PORT);
    }

    @Override
    public int getThriftPort()
    {
        return config.getInteger(CONFIG_THRIFT_LISTERN_PORT_NAME, DEFAULT_THRIFT_PORT);
    }

    @Override
    public int getStoragePort()
    {
        return config.getInteger(CONFIG_STORAGE_LISTERN_PORT_NAME, DEFAULT_STORAGE_PORT);
    }

    @Override
    public String getSnitch()
    {
        return config.getProperty(CONFIG_ENDPOINT_SNITCH, DEFULT_ENDPOINT_SNITCH);
    }

    @Override
    public String getAppName()
    {
        return config.getProperty(CONFIG_CLUSTER_NAME);
    }

    @Override
    public String getRac()
    {
        return RAC;
    }

    @Override
    public List<String> getRacs()
    {
        return config.getList(CONFIG_AVAILABILITY_ZONES);
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
        return config.getProperty(CONFIG_MAX_HEAP_SIZE + INSTANCE_TYPE, "2G");
    }

    @Override
    public String getHeapNewSize()
    {
        return config.getProperty(CONFIG_NEW_MAX_HEAP_SIZE + INSTANCE_TYPE, "2G");
    }

    @Override
    public int getBackupHour()
    {
        return config.getInteger(CONFIG_BACKUP_HOUR, DEFAULT_BACKUP_HOUR);
    }

    @Override
    public String getRestoreSnapshot()
    {
        return config.getProperty(CONFIG_AUTO_RESTORE_SNAPSHOTNAME, "");
    }

    @Override
    public String getDC()
    {
        return config.getProperty(CONFIG_REGION_NAME, "");
    }

    @Override
    public void setDC(String region)
    {
        config.setProperty(CONFIG_REGION_NAME, region);
    }

    @Override
    public boolean isMultiDC()
    {
        return config.getBoolean(CONFIG_MR_ENABLE, false);
    }

    @Override
    public int getMaxBackupUploadThreads()
    {

        return config.getInteger(CONFIG_BACKUP_THREADS, DEFAULT_BACKUP_THREADS);
    }

    @Override
    public int getMaxBackupDownloadThreads()
    {
        return config.getInteger(CONFIG_RESTORE_THREADS, DEFAULT_RESTORE_THREADS);
    }

    @Override
    public boolean isRestoreClosestToken()
    {
        return config.getBoolean(CONFIG_RESTORE_CLOSEST_TOKEN, false);
    }

    @Override
    public String getASGName()
    {
        return config.getProperty(CONFIG_ASG_NAME, "");
    }

    @Override
    public boolean isIncrBackup()
    {
        return config.getBoolean(CONFIG_INCR_BK_ENABLE, true);
    }

    @Override
    public String getHostIP()
    {
        return PUBLIC_IP;
    }

    @Override
    public int getUploadThrottle()
    {
        return config.getInteger(CONFIG_THROTTLE_UPLOAD_PER_SECOND, Integer.MAX_VALUE);
    }

    @Override
    public boolean isLocalBootstrapEnabled()
    {
        return config.getBoolean(CONFIG_LOAD_LOCAL_PROPERTIES, false);
    }

    @Override
    public int getInMemoryCompactionLimit()
    {
        return config.getInteger(CONFIG_IN_MEMORY_COMPACTION_LIMIT, 128);
    }

    @Override
    public int getCompactionThroughput()
    {
        return config.getInteger(CONFIG_COMPACTION_THROUHPUT, 8);
    }

    @Override
    public String getMaxDirectMemory()
    {
        return config.getProperty(CONFIG_DIRECT_MAX_HEAP_SIZE + INSTANCE_TYPE, "50G");
    }

    @Override
    public String getBootClusterName()
    {
        return config.getProperty(CONFIG_BOOTCLUSTER_NAME, "cass_turtle");
    }

    private class PriamProperties extends Properties
    {

        private static final long serialVersionUID = 1L;

        public int getInteger(String prop, int defaultValue)
        {
            return getProperty(prop) == null ? defaultValue : Integer.parseInt(getProperty(prop));
        }

        public long getLong(String prop, long defaultValue)
        {
            return getProperty(prop) == null ? defaultValue : Long.parseLong(getProperty(prop));
        }

        public boolean getBoolean(String prop, boolean defaultValue)
        {
            return getProperty(prop) == null ? defaultValue : Boolean.parseBoolean(getProperty(prop));
        }

        public List<String> getList(String prop)
        {
            if (getProperty(prop) == null)
                return Lists.newArrayList();
            return Arrays.asList(getProperty(prop).split(","));
        }

    }

}
