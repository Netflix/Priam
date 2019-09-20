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

import com.google.inject.ImplementedBy;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ImplementedBy(RemoteBackupPath.class)
public abstract class AbstractBackupPath implements Comparable<AbstractBackupPath> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBackupPath.class);
    public static final char PATH_SEP = File.separatorChar;

    protected String clusterName;
    protected String keyspace;
    protected String columnFamily;
    protected String fileName;
    protected String baseDir;
    protected String token;
    protected String region;
    protected Date time;
    protected UploadDownloadDirectives directives = new UploadDownloadDirectives();
    protected final InstanceIdentity instanceIdentity;
    protected final IConfiguration config;
    private File backupFile;
    private Instant lastModified;
    private Date uploadedTs;

    public AbstractBackupPath(IConfiguration config, InstanceIdentity instanceIdentity) {
        this.instanceIdentity = instanceIdentity;
        this.config = config;
    }

    public void parseLocal(File file, UploadDownloadDirectives.BackupFileType type)
            throws ParseException {
        this.backupFile = file;

        String rpath =
                new File(config.getDataFileLocation()).toURI().relativize(file.toURI()).getPath();
        String[] elements = rpath.split("" + PATH_SEP);
        this.clusterName = config.getAppName();
        this.baseDir = config.getBackupLocation();
        this.region = instanceIdentity.getInstanceInfo().getRegion();
        this.token = instanceIdentity.getInstance().getToken();
        if (UploadDownloadDirectives.BackupFileType.isDataFile(type)) {
            this.keyspace = elements[0];
            this.columnFamily = elements[1];
        }

        time = new Date(file.lastModified());

        /*
        1. For old style snapshots, make this value to time at which backup was executed.
        2. This is to ensure that all the files from the snapshot are uploaded under single directory in remote file system.
        3. For META file we always override the time field via @link{Metadata#decorateMetaJson}
        */
        if (type == UploadDownloadDirectives.BackupFileType.SNAP)
            time = DateUtil.getDate(elements[3]);

        this.lastModified = Instant.ofEpochMilli(file.lastModified());
        this.fileName = file.getName();
        directives.withSize(file.length()).withType(type).withLocalPath(file.toPath());
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
        StringBuilder buff = new StringBuilder();
        if (directives.getType() == UploadDownloadDirectives.BackupFileType.CL) {
            buff.append(config.getBackupCommitLogLocation()).append(PATH_SEP);
        } else {
            buff.append(config.getDataFileLocation()).append(PATH_SEP);
            if (directives.getType() != UploadDownloadDirectives.BackupFileType.META
                    && directives.getType() != UploadDownloadDirectives.BackupFileType.META_V2)
                buff.append(keyspace).append(PATH_SEP).append(columnFamily).append(PATH_SEP);
        }

        buff.append(fileName);

        File return_ = new File(buff.toString());
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

    public abstract Path remoteV2Prefix(
            Path location, UploadDownloadDirectives.BackupFileType fileType);

    public UploadDownloadDirectives getDirectives() {
        return directives;
    }

    public void setDirectives(UploadDownloadDirectives directives) {
        this.directives = directives;
    }

    /** Provides the cluster prefix */
    public abstract String clusterPrefix(String location);

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

    public String getBaseDir() {
        return baseDir;
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

    @Override
    public String toString() {
        return "From: " + getRemotePath() + " To: " + newRestoreFile().getPath();
    }

    public static class UploadDownloadDirectives {
        // Remote file system path denoting the location of the file.
        private Path remotePath;
        // Path to the local file system
        private Path localPath;
        // No of times to retry to upload/download a file from remote file system. If &lt;1, it will
        // perform operation exactly once.
        private int retry;
        // If true, delete the file denoted by localPath after it is successfully uploaded to the
        // filesystem. If there is any failure, file will not be deleted.
        private boolean deleteAfterSuccessfulUpload = false;
        // Compression technique  to be used for upload/download.
        private ICompression.CompressionAlgorithm compression =
                ICompression.CompressionAlgorithm.SNAPPY;
        // Encryption technique to be used for upload/download
        private IFileCryptography.CryptographyAlgorithm encryption =
                IFileCryptography.CryptographyAlgorithm.PLAINTEXT;
        // uncompressed and unencrypted file size in original form.
        private long size;
        // file size on the remote file system.
        private long compressedFileSize = 0;

        public enum BackupFileType {
            SNAP,
            SST,
            CL,
            META,
            META_V2,
            SST_V2;

            public static boolean isDataFile(BackupFileType type) {
                return type != BackupFileType.META
                        && type != BackupFileType.META_V2
                        && type != BackupFileType.CL;
            }
        }

        protected BackupFileType type;

        public Path getRemotePath() {
            return remotePath;
        }

        public UploadDownloadDirectives withRemotePath(Path remotePath) {
            this.remotePath = remotePath;
            return this;
        }

        public Path getLocalPath() {
            return localPath;
        }

        public UploadDownloadDirectives withLocalPath(Path localPath) {
            this.localPath = localPath;
            return this;
        }

        public int getRetry() {
            return retry;
        }

        public UploadDownloadDirectives withRetry(int retry) {
            this.retry = retry;
            return this;
        }

        public boolean isDeleteAfterSuccessfulUpload() {
            return deleteAfterSuccessfulUpload;
        }

        public UploadDownloadDirectives withDeleteAfterSuccessfulUpload(
                boolean deleteAfterSuccessfulUpload) {
            this.deleteAfterSuccessfulUpload = deleteAfterSuccessfulUpload;
            return this;
        }

        public ICompression.CompressionAlgorithm getCompression() {
            return compression;
        }

        public UploadDownloadDirectives withCompression(
                ICompression.CompressionAlgorithm compression) {
            this.compression = compression;
            return this;
        }

        public IFileCryptography.CryptographyAlgorithm getEncryption() {
            return encryption;
        }

        public UploadDownloadDirectives withEncryption(
                IFileCryptography.CryptographyAlgorithm encryption) {
            this.encryption = encryption;
            return this;
        }

        public long getSize() {
            return size;
        }

        public UploadDownloadDirectives withSize(long size) {
            this.size = size;
            return this;
        }

        public long getCompressedFileSize() {
            return this.compressedFileSize;
        }

        public UploadDownloadDirectives setCompressedFileSize(long val) {
            this.compressedFileSize = val;
            return this;
        }

        public BackupFileType getType() {
            return type;
        }

        public UploadDownloadDirectives withType(BackupFileType type) {
            this.type = type;
            return this;
        }
    }
}
