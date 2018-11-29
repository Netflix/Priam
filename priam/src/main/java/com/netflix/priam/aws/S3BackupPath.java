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
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/** Represents an S3 object key */
public class S3BackupPath extends AbstractBackupPath {

    @Inject
    public S3BackupPath(IConfiguration config, InstanceIdentity factory) {
        super(config, factory);
    }

    private Path getV2Prefix() {
        return Paths.get(baseDir, getAppNameWithHash(), instanceIdentity.getInstance().getToken());
    }

    private void parseV2Prefix(Path remoteFilePath) {
        if (remoteFilePath.getNameCount() < 3)
            throw new RuntimeException(
                    "Not enough no. of elements to parseV2Prefix : " + remoteFilePath);
        baseDir = remoteFilePath.getName(0).toString();
        clusterName = parseAndValidateAppNameWithHash(remoteFilePath.getName(1).toString());
        token = remoteFilePath.getName(2).toString();
    }

    /* This will ensure that there is some randomness in the path at the start so that remote file systems
    can hash the contents better when we have lot of clusters backing up at the same remote location.
    */
    private String getAppNameWithHash() {
        return String.format("%d_%s", config.getAppName().hashCode() % 10000, config.getAppName());
    }

    private String parseAndValidateAppNameWithHash(String appNameWithHash) {
        int hash = Integer.parseInt(appNameWithHash.substring(0, appNameWithHash.indexOf("_")));
        String appName = appNameWithHash.substring(appNameWithHash.indexOf("_") + 1);
        // Validate the hash
        int calculatedHash = appName.hashCode() % 10000;
        assert calculatedHash == hash;
        return appName;
    }

    /*
     * This method will generate the location for the V2 backups.
     * Note that we use epochMillis to sort the directory instead of traditional YYYYMMddHHmm. This will allow greater
     * flexibility when doing restores as the s3 list calls with common prefix will have greater chance of match instead
     * of traditional way (where it takes lot of s3 list calls when month or year changes).
     * Another major difference w.r.t. V1 is having no distinction between SNAP and SST files as we upload SSTables only
     * once to remote file system.
     */
    private Path getV2Location() {
        Path prefix =
                Paths.get(
                        getV2Prefix().toString(),
                        type.toString(),
                        getLastModified().toEpochMilli() + "");

        if (type == BackupFileType.SST_V2) {
            prefix = Paths.get(prefix.toString(), keyspace, columnFamily);
        }

        return Paths.get(prefix.toString(), fileName + "." + getCompression().toString());
    }

    private void parseV2Location(String remoteFile) {
        Path remoteFilePath = Paths.get(remoteFile);
        parseV2Prefix(remoteFilePath);
        assert remoteFilePath.getNameCount() >= 6;
        type = BackupFileType.valueOf(remoteFilePath.getName(3).toString());
        setLastModified(Instant.ofEpochMilli(Long.parseLong(remoteFilePath.getName(4).toString())));

        if (type == BackupFileType.SST_V2) {
            keyspace = remoteFilePath.getName(5).toString();
            columnFamily = remoteFilePath.getName(6).toString();
        }

        String fileNameCompression =
                remoteFilePath.getName(remoteFilePath.getNameCount() - 1).toString();
        fileName = fileNameCompression.substring(0, fileNameCompression.lastIndexOf("."));
        setCompression(
                ICompression.CompressionAlgorithm.valueOf(
                        fileNameCompression.substring(fileNameCompression.lastIndexOf(".") + 1)));
    }

    /**
     * Format of backup path: 1. For old style backups:
     * BASE/REGION/CLUSTER/TOKEN/[SNAPSHOTTIME]/[SST|SNAP|META]/KEYSPACE/COLUMNFAMILY/FILE
     *
     * <p>2. For new style backups (SnapshotMetaService)
     * BASE/[cluster_name_hash]_cluster/TOKEN//[META_V2|SST_V2]/KEYSPACE/COLUMNFAMILY/[last_modified_time_ms]/FILE.compression
     */
    @Override
    public String getRemotePath() {
        StringBuilder buff = new StringBuilder();
        if (type == BackupFileType.SST_V2 || type == BackupFileType.META_V2) {
            buff.append(getV2Location());
        } else {
            buff.append(baseDir).append(S3BackupPath.PATH_SEP); // Base dir
            buff.append(region).append(S3BackupPath.PATH_SEP);
            buff.append(clusterName).append(S3BackupPath.PATH_SEP); // Cluster name
            buff.append(token).append(S3BackupPath.PATH_SEP);
            buff.append(formatDate(time)).append(S3BackupPath.PATH_SEP);
            buff.append(type).append(S3BackupPath.PATH_SEP);
            if (BackupFileType.isDataFile(type))
                buff.append(keyspace)
                        .append(S3BackupPath.PATH_SEP)
                        .append(columnFamily)
                        .append(S3BackupPath.PATH_SEP);
            buff.append(fileName);
        }
        return buff.toString();
    }

    @Override
    public void parseRemote(String remoteFilePath) {
        // Check for all backup file types to ensure how we parse
        // TODO: We should clean this hack to get backupFileType for parsing when we delete V1 of
        // backups.
        for (BackupFileType fileType : BackupFileType.values()) {
            if (remoteFilePath.contains(PATH_SEP + fileType.toString() + PATH_SEP)) {
                type = fileType;
                break;
            }
        }

        if (type == BackupFileType.SST_V2 || type == BackupFileType.META_V2) {
            parseV2Location(remoteFilePath);
        } else {
            List<String> pieces = parsePrefix(remoteFilePath);
            assert pieces.size() >= 7 : "Too few elements in path " + remoteFilePath;
            time = parseDate(pieces.get(4));
            type = BackupFileType.valueOf(pieces.get(5));
            if (BackupFileType.isDataFile(type)) {
                keyspace = pieces.get(6);
                columnFamily = pieces.get(7);
            }
            // append the rest
            fileName = pieces.get(pieces.size() - 1);
        }
    }

    @Override
    public void parsePartialPrefix(String remoteFilePath) {
        parsePrefix(remoteFilePath);
    }

    private List<String> parsePrefix(String remoteFilePath) {
        String[] elements = remoteFilePath.split(String.valueOf(S3BackupPath.PATH_SEP));
        // parse out things which are empty
        List<String> pieces = Lists.newArrayList();
        for (String ele : elements) {
            if (ele.equals("")) continue;
            pieces.add(ele);
        }
        assert pieces.size() >= 4 : "Too few elements in path " + remoteFilePath;
        baseDir = pieces.get(0);
        region = pieces.get(1);
        clusterName = pieces.get(2);
        token = pieces.get(3);
        return pieces;
    }

    @Override
    public String remotePrefix(Date start, Date end, String location) {
        StringBuilder buff = new StringBuilder(clusterPrefix(location));
        token = instanceIdentity.getInstance().getToken();
        buff.append(token).append(S3BackupPath.PATH_SEP);
        // match the common characters to prefix.
        buff.append(match(start, end));
        return buff.toString();
    }

    @Override
    public String clusterPrefix(String location) {
        StringBuilder buff = new StringBuilder();
        String[] elements = location.split(String.valueOf(S3BackupPath.PATH_SEP));
        if (elements.length <= 1) {
            baseDir = config.getBackupLocation();
            region = instanceIdentity.getInstanceInfo().getRegion();
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
