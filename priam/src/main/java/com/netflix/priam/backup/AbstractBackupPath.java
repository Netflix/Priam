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
package com.netflix.priam.backup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.inject.ImplementedBy;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.config.BackupsToCompress;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cryptography.CryptographyAlgorithm;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

@ImplementedBy(RemoteBackupPath.class)
public abstract class AbstractBackupPath implements Comparable<AbstractBackupPath> {
    public static final char PATH_SEP = File.separatorChar;
    public static final Joiner PATH_JOINER = Joiner.on(PATH_SEP);

    public enum BackupFileType {
        CL,
        META,
        META_V2,
        SNAP,
        SNAPSHOT_VERIFIED,
        SST,
        SST_V2;

        private static final ImmutableSet<BackupFileType> DATA_FILE_TYPES =
                ImmutableSet.of(SNAP, SST, SST_V2);

        private static final ImmutableSet<BackupFileType> V2_FILE_TYPES =
                ImmutableSet.of(SST_V2, META_V2);

        public static boolean isDataFile(BackupFileType type) {
            return DATA_FILE_TYPES.contains(type);
        }

        public static boolean isV2(BackupFileType type) {
            return V2_FILE_TYPES.contains(type);
        }

        public static BackupFileType fromString(String s) throws BackupRestoreException {
            try {
                return BackupFileType.valueOf(s);
            } catch (IllegalArgumentException e) {
                throw new BackupRestoreException(String.format("Unknown BackupFileType %s", s));
            }
        }
    }

    protected BackupFileType type;
    protected String clusterName;
    protected String keyspace;
    protected String columnFamily;
    protected String fileName;
    protected String baseDir;
    protected String token;
    protected String region;
    protected Date time;
    private long size; // uncompressed file size
    private long compressedFileSize = 0;
    protected final InstanceIdentity instanceIdentity;
    protected final IConfiguration config;
    protected File backupFile;
    private Instant lastModified;
    private Instant creationTime;
    private Date uploadedTs;
    private CompressionType compression;
    private CryptographyAlgorithm encryption = CryptographyAlgorithm.PLAINTEXT;
    private boolean isIncremental;

    public AbstractBackupPath(IConfiguration config, InstanceIdentity instanceIdentity) {
        this.instanceIdentity = instanceIdentity;
        this.config = config;
        this.compression =
                config.getBackupsToCompress() == BackupsToCompress.NONE
                        ? CompressionType.NONE
                        : CompressionType.SNAPPY;
    }

    public void parseLocal(File file, BackupFileType type) {
        this.backupFile = file;
        this.baseDir = config.getBackupLocation();
        this.clusterName = config.getAppName();
        this.fileName = file.getName();
        BasicFileAttributes fileAttributes;
        try {
            fileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            this.lastModified = fileAttributes.lastModifiedTime().toInstant();
            this.creationTime = fileAttributes.creationTime().toInstant();
            this.size = fileAttributes.size();
        } catch (IOException e) {
            this.lastModified = Instant.ofEpochMilli(0L);
            this.creationTime = Instant.ofEpochMilli(0L);
            this.size = 0L;
        }
        this.region = instanceIdentity.getInstanceInfo().getRegion();
        this.token = instanceIdentity.getInstance().getToken();
        this.type = type;
        String rpath =
                new File(config.getDataFileLocation()).toURI().relativize(file.toURI()).getPath();
        String[] parts = rpath.split("" + PATH_SEP);
        if (BackupFileType.isDataFile(type)) {
            this.keyspace = parts[0];
            this.columnFamily = parts[1];
        }
        if (BackupFileType.isDataFile(type)) {
            Optional<BackupFolder> folder = BackupFolder.fromName(parts[2]);
            this.isIncremental = folder.filter(BackupFolder.BACKUPS::equals).isPresent();
        }

        /*
        1. For old style snapshots, make this value to time at which backup was executed.
        2. This is to ensure that all the files from the snapshot are uploaded under single directory in remote file system.
        3. For META file we always override the time field via @link{Metadata#decorateMetaJson}
        */
        this.time =
                type == BackupFileType.SNAP
                        ? DateUtil.getDate(parts[3])
                        : new Date(lastModified.toEpochMilli());
    }

    /** Given a date range, find a common string prefix Eg: 20120212, 20120213 = 2012021 */
    protected String match(Date start, Date end) {
        String sString = DateUtil.formatyyyyMMddHHmm(start); // formatDate(start);
        String eString = DateUtil.formatyyyyMMddHHmm(end); // formatDate(end);
        int diff = StringUtils.indexOfDifference(sString, eString);
        if (diff < 0) return sString;
        return sString.substring(0, diff);
    }

    /** Local restore file */
    public File newRestoreFile() {
        File return_;
        String dataDir = config.getDataFileLocation();
        switch (type) {
            case CL:
                return_ = new File(PATH_JOINER.join(config.getBackupCommitLogLocation(), fileName));
                break;
            case META:
            case META_V2:
                return_ = new File(PATH_JOINER.join(config.getDataFileLocation(), fileName));
                break;
            default:
                return_ = new File(PATH_JOINER.join(dataDir, keyspace, columnFamily, fileName));
        }

        File parent = new File(return_.getParent());
        if (!parent.exists()) parent.mkdirs();
        return return_;
    }

    @Override
    public int compareTo(AbstractBackupPath o) {
        return getRemotePath().compareTo(o.getRemotePath());
    }

    @Override
    public boolean equals(Object obj) {
        return obj.getClass().equals(this.getClass())
                && getRemotePath().equals(((AbstractBackupPath) obj).getRemotePath());
    }

    /** Get remote prefix for this path object */
    public abstract String getRemotePath();

    /** Parses a fully constructed remote path */
    public abstract void parseRemote(String remoteFilePath);

    /** Parses paths with just token prefixes */
    public abstract void parsePartialPrefix(String remoteFilePath);

    /**
     * Provides a common prefix that matches all objects that fall between the start and end time
     */
    public abstract String remotePrefix(Date start, Date end, String location);

    public abstract Path remoteV2Prefix(Path location, BackupFileType fileType);

    /** Provides the cluster prefix */
    public abstract String clusterPrefix(String location);

    public BackupFileType getType() {
        return type;
    }

    public void setType(BackupFileType type) {
        this.type = type;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getFileName() {
        return fileName;
    }

    public String getToken() {
        return token;
    }

    public String getRegion() {
        return region;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    /*
    @return original, uncompressed file size
     */
    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getCompressedFileSize() {
        return this.compressedFileSize;
    }

    public void setCompressedFileSize(long val) {
        this.compressedFileSize = val;
    }

    public File getBackupFile() {
        return backupFile;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public InstanceIdentity getInstanceIdentity() {
        return this.instanceIdentity;
    }

    public void setUploadedTs(Date uploadedTs) {
        this.uploadedTs = uploadedTs;
    }

    public Date getUploadedTs() {
        return this.uploadedTs;
    }

    public Instant getLastModified() {
        return lastModified;
    }

    public void setLastModified(Instant instant) {
        this.lastModified = instant;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    @VisibleForTesting
    public void setCreationTime(Instant instant) {
        this.creationTime = instant;
    }

    public CompressionType getCompression() {
        return compression;
    }

    public void setCompression(CompressionType compressionType) {
        this.compression = compressionType;
    }

    public CryptographyAlgorithm getEncryption() {
        return encryption;
    }

    public void setEncryption(String encryption) {
        this.encryption = CryptographyAlgorithm.valueOf(encryption);
    }

    public boolean isIncremental() {
        return isIncremental;
    }

    @Override
    public String toString() {
        return "From: " + getRemotePath() + " To: " + newRestoreFile().getPath();
    }
}
