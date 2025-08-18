/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.aws;

import com.netflix.priam.utils.SystemUtils;

/** Class for holding part data of a backup file, which will be used for multi-part uploading */
public class DataPart {
    private final String bucketName;
    private final String uploadID;
    private final String s3key;
    private int partNo;
    private byte[] partData;
    private byte[] md5;

    public DataPart(String bucket, String s3key, String mUploadId) {
        this.bucketName = bucket;
        this.uploadID = mUploadId;
        this.s3key = s3key;
    }

    public DataPart(int partNumber, byte[] data, String bucket, String s3key, String mUploadId) {
        this(bucket, s3key, mUploadId);
        this.partNo = partNumber;
        this.partData = data;
        this.md5 = SystemUtils.md5(data);
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getUploadID() {
        return uploadID;
    }

    public String getS3key() {
        return s3key;
    }

    public int getPartNo() {
        return partNo;
    }

    public byte[] getPartData() {
        return partData;
    }

    public byte[] getMd5() {
        return md5;
    }
}
