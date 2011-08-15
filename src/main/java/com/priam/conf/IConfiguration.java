package com.priam.conf;

import java.util.List;

public interface IConfiguration
{
    public void intialize();

    public String getYamlLocation();

    public String getBackupLocation();

    public String getBackupPrefix();

    public String getRestorePrefix();

    public String getDataFileLocation();

    public String getCacheLocation();

    public String getCommitLogLocation();

    public String getBackupCommitLogLocation();

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

    public boolean isMultiDC();

    public int getMaxBackupUploadThreads();

    public int getMaxBackupDownloadThreads();

    /**
     * Amazon specific setting to query ASG Membership
     * @return
     */
    public String getASGName();

    boolean isIncrBackup();

    public String getHostIP();
}