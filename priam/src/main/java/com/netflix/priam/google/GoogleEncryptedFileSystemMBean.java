package com.netflix.priam.google;

public interface GoogleEncryptedFileSystemMBean {

    String MBEAN_NAME = "com.priam.google.GoogleEncryptedFileSystemMBean:name=GoogleEncryptedFileSystemMBean";
    
    public int downloadCount();

    public int uploadCount();

    public int getActivecount();

    public long bytesUploaded();

    public long bytesDownloaded();
}