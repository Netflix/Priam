package com.netflix.priam.aws;

public interface S3FileSystemMBean {
    String MBEAN_NAME = "com.priam.aws.S3FileSystemMBean:name=S3FileSystemMBean";

    public int downloadCount();

    public int uploadCount();

    public int getActivecount();

    public long bytesUploaded();

    public long bytesDownloaded();
}
