package com.netflix.priam.aws;

public interface S3FileSystemMBean
{
    String MBEAN_NAME = "com.priam.aws.S3FileSystemMBean:name=S3FileSystemMBean";
    
    int downloadCount();

    int uploadCount();

    int getActivecount();

    long bytesUploaded();

    long bytesDownloaded();
}
