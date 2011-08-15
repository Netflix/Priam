package com.priam.aws;

import com.priam.utils.SystemUtils;

public class DataPart
{
    public final String bucketName;
    public final String uploadID;
    public final String s3key;
    public int partNo;
    public byte[] partData;
    public byte[] md5;
    
    /**
     * make sure to use this in the right place. 
     */
    public DataPart(String bucket, String s3key, String mUploadId)
    {
        this.bucketName = bucket;
        this.uploadID = mUploadId;
        this.s3key = s3key;
    }
    
    public DataPart(int partNumber, byte[] data, String bucket, String s3key, String mUploadId)
    {
        this(bucket, s3key, mUploadId);
        this.partNo = partNumber;
        this.partData = data;
        this.md5 = SystemUtils.md5(data);
    }
}
