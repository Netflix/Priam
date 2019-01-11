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

import com.google.inject.Inject;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;

/**
 * Represents location of an object on the remote file system. All the objects will be keyed with a
 * common prefix (based on configuration, typically environment), name of the cluster and token of
 * this instance.
 */
public class RemoteBackupPath extends AbstractBackupPath {

    @Inject
    public RemoteBackupPath(IConfiguration config, InstanceIdentity factory) {
        super(config, factory);
    }

    private Path getV2Prefix() {
        return Paths.get(baseDir, getAppNameWithHash(clusterName), token);
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
    private String getAppNameWithHash(String appName) {
        return String.format("%d_%s", appName.hashCode() % 10000, appName);
    }

    private String parseAndValidateAppNameWithHash(String appNameWithHash) {
        int hash = Integer.parseInt(appNameWithHash.substring(0, appNameWithHash.indexOf("_")));
        String appName = appNameWithHash.substring(appNameWithHash.indexOf("_") + 1);
        // Validate the hash
        int calculatedHash = appName.hashCode() % 10000;
        if (calculatedHash != hash)
            throw new RuntimeException(
                    String.format(
                            "Hash for the app name: %s was calculated to be: %d but provided was: %d",
                            appName, calculatedHash, hash));
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

        return Paths.get(
                prefix.toString(),
                getCompression().toString(),
                getEncryption().toString(),
                fileName);
    }

    private void parseV2Location(String remoteFile) {
        Path remoteFilePath = Paths.get(remoteFile);
        parseV2Prefix(remoteFilePath);
        if (remoteFilePath.getNameCount() < 8)
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Too few elements (expected: [%d]) in path: %s", 8, remoteFilePath));
        int name_count_idx = 3;

        type = BackupFileType.valueOf(remoteFilePath.getName(name_count_idx++).toString());
        setLastModified(
                Instant.ofEpochMilli(
                        Long.parseLong(remoteFilePath.getName(name_count_idx++).toString())));

        if (type == BackupFileType.SST_V2) {
            keyspace = remoteFilePath.getName(name_count_idx++).toString();
            columnFamily = remoteFilePath.getName(name_count_idx++).toString();
        }

        setCompression(
                ICompression.CompressionAlgorithm.valueOf(
                        remoteFilePath.getName(name_count_idx++).toString()));

        setEncryption(
                IFileCryptography.CryptographyAlgorithm.valueOf(
                        remoteFilePath.getName(name_count_idx++).toString()));
        fileName = remoteFilePath.getName(name_count_idx).toString();
    }

    private Path getV1Location() {
        Path path =
                Paths.get(
                        getV1Prefix().toString(),
                        DateUtil.formatyyyyMMddHHmm(time),
                        type.toString());
        if (BackupFileType.isDataFile(type))
            path = Paths.get(path.toString(), keyspace, columnFamily);
        return Paths.get(path.toString(), fileName);
    }

    private void parseV1Location(Path remoteFilePath) {
        parseV1Prefix(remoteFilePath);
        if (remoteFilePath.getNameCount() < 7)
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Too few elements (expected: [%d]) in path: %s", 7, remoteFilePath));

        time = DateUtil.getDate(remoteFilePath.getName(4).toString());
        type = BackupFileType.valueOf(remoteFilePath.getName(5).toString());
        if (BackupFileType.isDataFile(type)) {
            keyspace = remoteFilePath.getName(6).toString();
            columnFamily = remoteFilePath.getName(7).toString();
        }
        // append the rest
        fileName = remoteFilePath.getName(remoteFilePath.getNameCount() - 1).toString();
    }

    private Path getV1Prefix() {
        return Paths.get(baseDir, region, clusterName, token);
    }

    private void parseV1Prefix(Path remoteFilePath) {
        if (remoteFilePath.getNameCount() < 4)
            throw new RuntimeException(
                    "Not enough no. of elements to parseV1Prefix : " + remoteFilePath);
        baseDir = remoteFilePath.getName(0).toString();
        region = remoteFilePath.getName(1).toString();
        clusterName = remoteFilePath.getName(2).toString();
        token = remoteFilePath.getName(3).toString();
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
        if (type == BackupFileType.SST_V2 || type == BackupFileType.META_V2) {
            return getV2Location().toString();
        } else {
            return getV1Location().toString();
        }
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
            parseV1Location(Paths.get(remoteFilePath));
        }
    }

    @Override
    public void parsePartialPrefix(String remoteFilePath) {
        parseV1Prefix(Paths.get(remoteFilePath));
    }

    @Override
    public String remotePrefix(Date start, Date end, String location) {
        StringBuilder buff = new StringBuilder(clusterPrefix(location));
        token = instanceIdentity.getInstance().getToken();
        buff.append(token).append(RemoteBackupPath.PATH_SEP);
        // match the common characters to prefix.
        buff.append(match(start, end));
        return buff.toString();
    }

    @Override
    public Path remoteV2Prefix(Path location, BackupFileType fileType) {
        if (location.getNameCount() <= 1) {
            baseDir = config.getBackupLocation();
            clusterName = config.getAppName();
        } else if (location.getNameCount() >= 3) {
            baseDir = location.getName(1).toString();
            clusterName = parseAndValidateAppNameWithHash(location.getName(2).toString());
        }
        token = instanceIdentity.getInstance().getToken();
        return Paths.get(getV2Prefix().toString(), fileType.toString());
    }

    @Override
    public String clusterPrefix(String location) {
        StringBuilder buff = new StringBuilder();
        String[] elements = location.split(String.valueOf(RemoteBackupPath.PATH_SEP));
        if (elements.length <= 1) {
            baseDir = config.getBackupLocation();
            region = instanceIdentity.getInstanceInfo().getRegion();
            clusterName = config.getAppName();
        } else {
            if (elements.length < 4)
                throw new IndexOutOfBoundsException(
                        String.format(
                                "Too few elements (expected: [%d]) in path: %s", 4, location));
            baseDir = elements[1];
            region = elements[2];
            clusterName = elements[3];
        }
        buff.append(baseDir).append(RemoteBackupPath.PATH_SEP);
        buff.append(region).append(RemoteBackupPath.PATH_SEP);
        buff.append(clusterName).append(RemoteBackupPath.PATH_SEP);

        return buff.toString();
    }
}
