package com.priam.conf;

import java.util.List;

public interface IConfiguration
{
    public void intialize();

    public String getCassHome();
    
    public String getCassStartupScript();

    public String getBackupLocation();

    public String getBackupPrefix();

    public String getRestorePrefix();
    
    public List<String> getRestoreKeySpaces();

    public String getDataFileLocation();

    public String getCacheLocation();

    public String getCommitLogLocation();

    public String getBackupCommitLogLocation();
    
    public long getBackupChunkSize();

    public boolean isCommitLogBackup();

    public int getJmxPort();

    public int getThriftPort();

    public String getSnitch();

    public String getAppName();

    public String getRac();

    public List<String> getRacs();

    public String getHostname();

    public String getInstanceName();

    public String getHeapSize();

    public String getHeapNewSize();

    public int getBackupHour();

    public String getRestoreSnapshot();

    public boolean isExperimental();

    public String getDC();
    
    public void setDC(String region);

    public boolean isMultiDC();

    public int getMaxBackupUploadThreads();

    public int getMaxBackupDownloadThreads();
    
    public boolean isRestoreClosestToken();

    /**
     * Amazon specific setting to query ASG Membership
     */
    public String getASGName();

    boolean isIncrBackup();

    public String getHostIP();

    public int getUploadThrottle();
    
    boolean isLocalBootstrapEnabled();

    public int getInMemoryCompactionLimit();

    public int getCompactionThroughput();

    public String getMaxDirectMemory();
    
    public String getBootClusterName();
    
    public int getCommitLogBackupPort();
}