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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Represents location of an object on the remote file system. All the objects will be keyed with a
 * common prefix (based on configuration, typically environment), name of the cluster and token of
 * this instance.
 */
public class RemoteBackupPath extends AbstractBackupPath {
    private static final ImmutableSet<BackupFileType> V2_ONLY_FILE_TYPES =
            ImmutableSet.of(BackupFileType.META_V2, BackupFileType.SST_V2);

    @Inject
    public RemoteBackupPath(IConfiguration config, InstanceIdentity factory) {
        super(config, factory);
    }

    private ImmutableList.Builder<String> getV2Prefix() {
        ImmutableList.Builder<String> prefix = ImmutableList.builder();
        prefix.add(baseDir, prependHash(clusterName), token);
        return prefix;
    }

    /* This will ensure that there is some randomness in the path at the start so that remote file systems
    can hash the contents better when we have lot of clusters backing up at the same remote location.
    */
    private String prependHash(String appName) {
        return String.format("%d_%s", appName.hashCode() % 10000, appName);
    }

    private String removeHash(String appNameWithHash) {
        int hash = Integer.parseInt(appNameWithHash.substring(0, appNameWithHash.indexOf("_")));
        String appName = appNameWithHash.substring(appNameWithHash.indexOf("_") + 1);
        Preconditions.checkArgument(
                hash == appName.hashCode() % 10000,
                "Prepended hash does not match app name. Should have received: "
                        + prependHash(appName));
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
    private String getV2Location() {
        ImmutableList.Builder<String> parts = getV2Prefix();
        // JDK-8177809 truncate to seconds to ensure consistent behavior with our old method of
        // getting lastModified time (File::lastModified) in Java 8.
        long lastModified = getLastModified().toEpochMilli() / 1_000L * 1_000L;
        parts.add(type.toString(), lastModified + "");
        if (BackupFileType.isDataFile(type)) {
            parts.add(keyspace, columnFamily);
        }
        parts.add(getCompression().toString(), getEncryption().toString(), fileName);
        return toPath(parts.build()).toString();
    }

    private void parseV2Location(Path remotePath) {
        Preconditions.checkArgument(
                remotePath.getNameCount() >= 8,
                String.format("%s has fewer than %d parts", remotePath, 8));
        int index = 0;
        baseDir = remotePath.getName(index++).toString();
        clusterName = removeHash(remotePath.getName(index++).toString());
        token = remotePath.getName(index++).toString();
        type = BackupFileType.valueOf(remotePath.getName(index++).toString());
        String lastModified = remotePath.getName(index++).toString();
        setLastModified(Instant.ofEpochMilli(Long.parseLong(lastModified)));
        List<String> parts = Lists.newArrayListWithCapacity(4);
        if (BackupFileType.isDataFile(type)) {
            keyspace = remotePath.getName(index++).toString();
            columnFamily = remotePath.getName(index++).toString();
            parts.add(keyspace);
            parts.add(columnFamily);
        }
        setCompression(CompressionType.valueOf(remotePath.getName(index++).toString()));
        setEncryption(remotePath.getName(index++).toString());
        fileName = remotePath.getName(index).toString();
        parts.add(fileName);
        this.backupFile =
                Paths.get(config.getDataFileLocation(), parts.toArray(new String[] {})).toFile();
    }

    private String getV1Location() {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        String timeString = DateUtil.formatyyyyMMddHHmm(time);
        parts.add(baseDir, region, clusterName, token, timeString, type.toString());
        if (BackupFileType.isDataFile(type)) {
            parts.add(keyspace, columnFamily);
        }
        parts.add(fileName);
        return toPath(parts.build()).toString();
    }

    private Path toPath(ImmutableList<String> parts) {
        return Paths.get(parts.get(0), parts.subList(1, parts.size()).toArray(new String[0]));
    }

    private void parseV1Location(Path remotePath) {
        Preconditions.checkArgument(
                remotePath.getNameCount() >= 7,
                String.format("%s has fewer than %d parts", remotePath, 7));
        parseV1Prefix(remotePath);
        time = DateUtil.getDate(remotePath.getName(4).toString());
        type = BackupFileType.valueOf(remotePath.getName(5).toString());
        if (BackupFileType.isDataFile(type)) {
            keyspace = remotePath.getName(6).toString();
            columnFamily = remotePath.getName(7).toString();
        }
        fileName = remotePath.getName(remotePath.getNameCount() - 1).toString();
    }

    private void parseV1Prefix(Path remotePath) {
        Preconditions.checkArgument(
                remotePath.getNameCount() >= 4,
                String.format("%s needs %d parts to parse prefix", remotePath, 4));
        baseDir = remotePath.getName(0).toString();
        region = remotePath.getName(1).toString();
        clusterName = remotePath.getName(2).toString();
        token = remotePath.getName(3).toString();
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
        return V2_ONLY_FILE_TYPES.contains(type) ? getV2Location() : getV1Location();
    }

    @Override
    public void parseRemote(String remotePath) {
        // Hack to determine type in advance of parsing. Will disappear once v1 is retired
        Optional<BackupFileType> inferredType =
                Arrays.stream(BackupFileType.values())
                        .filter(bft -> remotePath.contains(PATH_SEP + bft.toString() + PATH_SEP))
                        .findAny()
                        .filter(V2_ONLY_FILE_TYPES::contains);
        if (inferredType.isPresent()) {
            parseV2Location(Paths.get(remotePath));
        } else {
            parseV1Location(Paths.get(remotePath));
        }
    }

    @Override
    public void parsePartialPrefix(String remoteFilePath) {
        parseV1Prefix(Paths.get(remoteFilePath));
    }

    @Override
    public String remotePrefix(Date start, Date end, String location) {
        return PATH_JOINER.join(
                clusterPrefix(location),
                instanceIdentity.getInstance().getToken(),
                match(start, end));
    }

    @Override
    public Path remoteV2Prefix(Path location, BackupFileType fileType) {
        if (location.getNameCount() <= 1) {
            baseDir = config.getBackupLocation();
            clusterName = config.getAppName();
        } else if (location.getNameCount() >= 3) {
            baseDir = location.getName(1).toString();
            clusterName = removeHash(location.getName(2).toString());
        }
        token = instanceIdentity.getInstance().getToken();
        ImmutableList.Builder<String> parts = getV2Prefix();
        parts.add(fileType.toString());
        return toPath(parts.build());
    }

    @Override
    public String clusterPrefix(String location) {
        String[] elements = location.split(String.valueOf(RemoteBackupPath.PATH_SEP));
        Preconditions.checkArgument(
                elements.length < 2 || elements.length > 3,
                "Path must have fewer than 2 or greater than 3 elements. Saw " + location);
        if (elements.length <= 1) {
            baseDir = config.getBackupLocation();
            region = instanceIdentity.getInstanceInfo().getRegion();
            clusterName = config.getAppName();
        } else {
            baseDir = elements[1];
            region = elements[2];
            clusterName = elements[3];
        }
        return PATH_JOINER.join(baseDir, region, clusterName, ""); // "" ensures a trailing "/"
    }
}
