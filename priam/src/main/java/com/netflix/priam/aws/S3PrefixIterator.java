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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Class to iterate over prefixes (S3 Common prefixes) upto 
 * the token element in the path. The abstract path generated by this class
 * is partial (does not have all data). 
 */
public class S3PrefixIterator implements Iterator<AbstractBackupPath> {
    private static final Logger logger = LoggerFactory.getLogger(S3PrefixIterator.class);
    private final IConfiguration config;
    private final AmazonS3 s3Client;
    private final Provider<AbstractBackupPath> pathProvider;
    private Iterator<AbstractBackupPath> iterator;

    private String bucket = "";
    private String clusterPath = "";
    private SimpleDateFormat datefmt = new SimpleDateFormat("yyyyMMdd");
    private ObjectListing objectListing = null;
    Date date;

    @Inject
    public S3PrefixIterator(IConfiguration config, Provider<AbstractBackupPath> pathProvider, AmazonS3 s3Client, Date date) {
        this.config = config;
        this.pathProvider = pathProvider;
        this.s3Client = s3Client;
        this.date = date;
        String path = "";
        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            path = config.getRestorePrefix();
        else
            path = config.getBackupPrefix();

        String[] paths = path.split(String.valueOf(S3BackupPath.PATH_SEP));
        bucket = paths[0];
        this.clusterPath = remotePrefix(path);
        iterator = createIterator();
    }

    private void initListing() {
        ListObjectsRequest listReq = new ListObjectsRequest();
        // Get list of tokens
        listReq.setBucketName(bucket);
        listReq.setPrefix(clusterPath);
        listReq.setDelimiter(String.valueOf(AbstractBackupPath.PATH_SEP));
        logger.info("Using cluster prefix for searching tokens: " + clusterPath);
        objectListing = s3Client.listObjects(listReq);

    }

    private Iterator<AbstractBackupPath> createIterator() {
        if (objectListing == null)
            initListing();
        List<AbstractBackupPath> temp = Lists.newArrayList();
        for (String summary : objectListing.getCommonPrefixes()) {
            if (pathExistsForDate(summary, datefmt.format(date))) {
                AbstractBackupPath path = pathProvider.get();
                path.parsePartialPrefix(summary);
                temp.add(path);
            }
        }
        return temp.iterator();
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            return true;
        } else {
            while (objectListing.isTruncated() && !iterator.hasNext()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
                iterator = createIterator();
            }
        }
        return iterator.hasNext();
    }

    @Override
    public AbstractBackupPath next() {
        return iterator.next();
    }

    @Override
    public void remove() {
    }

    /**
     * Get remote prefix upto the token
     */
    private String remotePrefix(String location) {
        StringBuffer buff = new StringBuffer();
        String[] elements = location.split(String.valueOf(S3BackupPath.PATH_SEP));
        if (elements.length <= 1) {
            buff.append(config.getBackupLocation()).append(S3BackupPath.PATH_SEP);
            buff.append(config.getDC()).append(S3BackupPath.PATH_SEP);
            buff.append(config.getAppName()).append(S3BackupPath.PATH_SEP);
        } else {
            assert elements.length >= 4 : "Too few elements in path " + location;
            buff.append(elements[1]).append(S3BackupPath.PATH_SEP);
            buff.append(elements[2]).append(S3BackupPath.PATH_SEP);
            buff.append(elements[3]).append(S3BackupPath.PATH_SEP);
        }
        return buff.toString();
    }

    /**
     * Check to see if the path exists for the date
     */
    private boolean pathExistsForDate(String tprefix, String datestr) {
        ListObjectsRequest listReq = new ListObjectsRequest();
        // Get list of tokens
        listReq.setBucketName(bucket);
        listReq.setPrefix(tprefix + datestr);
        ObjectListing listing;
        listing = s3Client.listObjects(listReq);
        if (listing.getObjectSummaries().size() > 0)
            return true;
        return false;
    }

}
