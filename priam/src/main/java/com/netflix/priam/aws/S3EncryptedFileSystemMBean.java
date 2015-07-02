package com.netflix.priam.aws;

public interface S3EncryptedFileSystemMBean {

    String ENCRYPTED_FILE_SYSTEM_MBEAN_NAME = "com.priam.aws.S3EncryptedFileSystemMBean:name=S3EncryptedFileSystemMBean";
    
    public int downloadCount();

    public int uploadCount();

    public int getActivecount();

    public long bytesUploaded();

    public long bytesDownloaded();
}