/*
 * Copyright 2018 Netflix, Inc.
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

import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterate over the s3 file system. This is really required to find the manifest file for restore
 * and downloading incrementals. Created by aagrawal on 11/30/18.
 */
public class S3Iterator implements Iterator<String> {
    private Iterator<String> iterator;
    private ListObjectsResponse objectListing;
    private final S3Client s3Client;
    private final String bucket;
    private final String prefix;
    private final String delimiter;
    private final String marker;

    public S3Iterator(
            S3Client s3Client, String bucket, String prefix, String delimiter, String marker) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.prefix = prefix;
        this.delimiter = delimiter;
        this.marker = marker;
        iterator = createIterator();
    }

    private void initListing() {
        ListObjectsRequest.Builder listReqBuilder = ListObjectsRequest.builder()
                .bucket(bucket)
                .prefix(prefix);

        if (StringUtils.isNotBlank(delimiter)) listReqBuilder.delimiter(delimiter);
        if (StringUtils.isNotBlank(marker)) listReqBuilder.marker(marker);

        objectListing = s3Client.listObjects(listReqBuilder.build());
    }

    private Iterator<String> createIterator() {
        if (objectListing == null) initListing();
        List<String> temp = Lists.newArrayList();
        for (S3Object summary : objectListing.contents()) {
            temp.add(summary.key());
        }
        return temp.iterator();
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            return true;
        } else {
            while (objectListing.isTruncated() && !iterator.hasNext()) {
                ListObjectsRequest.Builder nextReqBuilder = ListObjectsRequest.builder()
                        .bucket(bucket)
                        .prefix(prefix)
                        .marker(objectListing.nextMarker());

                if (StringUtils.isNotBlank(delimiter)) nextReqBuilder.delimiter(delimiter);

                objectListing = s3Client.listObjects(nextReqBuilder.build());
                iterator = createIterator();
            }
        }
        return iterator.hasNext();
    }

    @Override
    public String next() {
        return iterator.next();
    }
}