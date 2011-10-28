package com.priam.netflix.monitoring;

import com.netflix.monitoring.DataSourceType;
import com.netflix.monitoring.Monitor;
import com.priam.aws.S3FileSystemMBean;

/**
 * Monitor the S3 File system on the following metrics (self explanatory)
 * 
 * @author "Vijay Parthasarathy"
 */
public class FileSystemMonitor extends AbstractMonitor<S3FileSystemMBean>
{
    public FileSystemMonitor(String name)
    {
        super("FileSystem_" + name);
    }

    @Monitor(dataSourceName = "FilesDownloadCount", type = DataSourceType.COUNTER)
    public int downloadCount()
    {
        return bean.downloadCount();
    }

    @Monitor(dataSourceName = "FilesUploadCount", type = DataSourceType.COUNTER)
    public int uploadCount()
    {
        return bean.uploadCount();
    }

    @Monitor(dataSourceName = "getActivecount", type = DataSourceType.COUNTER)
    public int getActivecount()
    {
        return bean.getActivecount();
    }

    @Monitor(dataSourceName = "bytesUploaded", type = DataSourceType.COUNTER)
    public long bytesUploaded()
    {
        return bean.bytesUploaded();
    }

    @Monitor(dataSourceName = "bytesDownloaded", type = DataSourceType.COUNTER)
    public long bytesDownloaded()
    {
        return bean.bytesDownloaded();
    }
}
