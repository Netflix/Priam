package com.netflix.priam;

import java.util.List;

/**
 * Interface for Priam's configuration
 * 
 */
public interface IConfiguration
{
    public void intialize();

    /**
     * @return Path to the home dir of Cassandra
     */
    public String getCassHome();

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
     * 
     * @return
     */
    public String getRestorePrefix();

    /**
     * @return List of keyspaces to restore. If none, all keyspaces are
     *         restored.
     */
    public List<String> getRestoreKeySpaces();

    /**
     * @return Location of the local data dir
     */
    public String getDataFileLocation();

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
     * @return trus if commit log backup is enabled
     */
    public boolean isCommitLogBackup();

    /**
     * @return Cassandra's JMX port
     */
    public int getJmxPort();
        
    /**
     * Cassandra storage/cluster communication port
     * @return
     */
    public int getStoragePort();

    /**
     * @return Cassandra's thrift port
     */
    public int getThriftPort();

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
     * @param region
     *            Set the current data center
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
     *         is not found
     */
    public boolean isRestoreClosestToken();

    /**
     * Amazon specific setting to query ASG Membership
     */
    public String getASGName();

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
     * @return hinted_handoff_throttle_delay_in_ms
     */
    public int getHintHandoffDelay();
    
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
     * @return stream_throughput_outbound_megabits_per_sec in yaml
     */
    public int getStreamingThroughputMB();
    
    /**
     * @return multithreaded_compaction in yaml
     */
    public boolean getMultithreadedCompaction();

    /**
     * Retrieve the partitioner in use.
     *
     * @return fully-qualified java class name of partitioner.
     */
    public String getPartitioner();
}