package com.priam.aws;

public interface S3FileSystemMBean
{
    int downloadCount();

    int uploadCount();

    int getActivecount();

    long bytesUploaded();

    long bytesDownloaded();
}
