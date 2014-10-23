package com.netflix.priam.google;

public interface GoogleFileSystemMBean {

    String MBEAN_NAME = "com.priam.google.GoogleFileSystemMBean:name=GoogleFileSystemMBean";
    
    public int downloadCount();

    public int uploadCount();

    public int getActivecount();

    public long bytesUploaded();

    public long bytesDownloaded();
}