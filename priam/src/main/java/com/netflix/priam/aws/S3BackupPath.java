/*
 * Copyright 2017 Netflix, Inc.
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
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.identity.InstanceIdentity;

import java.util.Date;
import java.util.List;

/**
 * Represents an S3 object key
 */
public class S3BackupPath extends AbstractBackupPath {
    /*
     * Checking if request came from Cassandra 1.0 or 1.1 
     * In Cassandra 1.0, Number of path elements = 8
     * In Cassandra 1.1, Number of path elements = 9
     */
    public static final int NUM_PATH_ELEMENTS_CASS_1_0 = 8;

    @Inject
    public S3BackupPath(IConfiguration config, InstanceIdentity factory) {
        super(config, factory);
    }

    /**
     * Format of backup path:
     * Cassandra 1.0
     * BASE/REGION/CLUSTER/TOKEN/[SNAPSHOTTIME]/[SST|SNP|META]/KEYSPACE/FILE
     * Cassandra 1.1
     * BASE/REGION/CLUSTER/TOKEN/[SNAPSHOTTIME]/[SST|SNP|META]/KEYSPACE/COLUMNFAMILY/FILE
     */
    @Override
    public String getRemotePath() {
        StringBuffer buff = new StringBuffer();
        buff.append(baseDir).append(S3BackupPath.PATH_SEP); // Base dir
        buff.append(region).append(S3BackupPath.PATH_SEP);
        buff.append(clusterName).append(S3BackupPath.PATH_SEP);// Cluster name
        buff.append(nodeIdentifier).append(S3BackupPath.PATH_SEP);
        buff.append(formatDate(time)).append(S3BackupPath.PATH_SEP);
        buff.append(type).append(S3BackupPath.PATH_SEP);
        if (type != BackupFileType.META && type != BackupFileType.CL) {
            if (isCassandra1_0)
                buff.append(keyspace).append(S3BackupPath.PATH_SEP);
            else
                buff.append(keyspace).append(S3BackupPath.PATH_SEP).append(columnFamily).append(S3BackupPath.PATH_SEP);
        }
        buff.append(fileName);
        return buff.toString();
    }

    @Override
    public void parseRemote(String remoteFilePath) {
        String[] elements = remoteFilePath.split(String.valueOf(S3BackupPath.PATH_SEP));
        // parse out things which are empty
        List<String> pieces = Lists.newArrayList();
        for (String ele : elements) {
            if (ele.equals(""))
                continue;
            pieces.add(ele);
        }
        assert pieces.size() >= 7 : "Too few elements in path " + remoteFilePath;
        if (pieces.size() == NUM_PATH_ELEMENTS_CASS_1_0)
            setCassandra1_0(true);
        baseDir = pieces.get(0);
        region = pieces.get(1);
        clusterName = pieces.get(2);
        nodeIdentifier = pieces.get(3);
        time = parseDate(pieces.get(4));
        type = BackupFileType.valueOf(pieces.get(5));
        if (type != BackupFileType.META && type != BackupFileType.CL) {
            keyspace = pieces.get(6);
            if (!isCassandra1_0)
                columnFamily = pieces.get(7);
        }
        // append the rest
        fileName = pieces.get(pieces.size() - 1);
    }

    @Override
    public void parsePartialPrefix(String remoteFilePath) {
        String[] elements = remoteFilePath.split(String.valueOf(S3BackupPath.PATH_SEP));
        // parse out things which are empty
        List<String> pieces = Lists.newArrayList();
        for (String ele : elements) {
            if (ele.equals(""))
                continue;
            pieces.add(ele);
        }
        assert pieces.size() >= 4 : "Too few elements in path " + remoteFilePath;
        baseDir = pieces.get(0);
        region = pieces.get(1);
        clusterName = pieces.get(2);
        nodeIdentifier = pieces.get(3);
    }

    @Override
    public String remotePrefix(Date start, Date end, String location) {
        StringBuffer buff = new StringBuffer(clusterPrefix(location));
        nodeIdentifier = instanceIdentity.getBackupIdentifier();
        buff.append(nodeIdentifier).append(S3BackupPath.PATH_SEP);
        // match the common characters to prefix.
        buff.append(match(start, end));
        return buff.toString();
    }

    @Override
    public String clusterPrefix(String location) {
        StringBuffer buff = new StringBuffer();
        String[] elements = location.split(String.valueOf(S3BackupPath.PATH_SEP));
        if (elements.length <= 1) {
            baseDir = config.getBackupLocation();
            region = config.getDC();
            clusterName = config.getAppName();
        } else {
            assert elements.length >= 4 : "Too few elements in path " + location;
            baseDir = elements[1];
            region = elements[2];
            clusterName = elements[3];
        }
        buff.append(baseDir).append(S3BackupPath.PATH_SEP);
        buff.append(region).append(S3BackupPath.PATH_SEP);
        buff.append(clusterName).append(S3BackupPath.PATH_SEP);

        return buff.toString();
    }

}
