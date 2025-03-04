/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.priam.backupv2;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.netflix.priam.aws.S3Iterator;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.RangeReadInputStream;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

// TODO filesystem methods (download, delete, list), deal with basedir, read meta, inject a clock
// TODO opportunities to split out:  S3 interactions, local file interactions, a new class that represents a backup path
@Singleton
public class BackupTTLTask {
    private static final Logger log = LoggerFactory.getLogger(BackupTTLTask.class);
    private List<String> filesToDelete = new ArrayList<>();
    private final int BATCH_SIZE = 1000;
    // TODO our LIST calls might be cheaper if this was later. Look into that.
    private static final Instant THE_START = Instant.parse("2018-01-01T00:00:00Z");
    private static final int BACKUP_TYPE_INDEX = 3;
    private static final int LAST_MODIFIED_INDEX = 4;
    private static final Splitter SPLITTER = Splitter.on('/');
    private static final Joiner JOINER = Joiner.on('/');
    private final BlobStore blobStore;

    @Inject
    public BackupTTLTask(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    public void execute(String appName, String token, int retentionDays, int gracePeriodDays) throws Exception {
        filesToDelete.clear();
        // TODO inject clock
        Instant now = Instant.now();
        String globalPrefix = JOINER.join(baseDir, prependHash(appName), token);
        Optional<String> oldestMeta = getOldestMetaFile(now, globalPrefix, retentionDays);
        if (!oldestMeta.isPresent()) {
            return;
        }
        Path localFile = downloadFile(oldestMeta.get(), 10 /* retries */);
        ImmutableSet<String> filesInMeta = getFilesFromMeta(localFile);
        FileUtils.deleteQuietly(localFile.toFile());
        if (filesInMeta.isEmpty()) {
            return;
        }

        Iterator<String> remoteFileLocations =
                new S3Iterator(s3Client, getShard(), JOINER.join(globalPrefix, FileType.SST_V2.toString()), null, null);
        Instant dateToTtl = now.minus(retentionDays + gracePeriodDays, ChronoUnit.DAYS);
        while (remoteFileLocations.hasNext()) {
            String path = remoteFileLocations.next();
            if (getLastModified(path).isAfter(dateToTtl)) {
                break;
            }
            if (!filesInMeta.contains(removeCompressionPart(path))) {
                deleteFile(path);
            }
        }
        deleteFiles();
    }

    private Optional<String> getOldestMetaFile(Instant now, String globalPrefix, int retentionDays) {
        List<String> metas = getAllMetaFiles(now, JOINER.join(globalPrefix, FileType.META_V2.toString()));
        Instant dateToTtl = now.minus(retentionDays, ChronoUnit.DAYS);
        if (getLastModified(metas.get(metas.size() - 1)).isBefore(dateToTtl)) {
            return Optional.empty();
        }
        String oldestMeta = null;
        for (String meta : metas) {
            if (getLastModified(meta).isBefore(dateToTtl)) {
                deleteFile(meta);
            } else {
                oldestMeta = meta;
                break;
            }
        }
        return Optional.ofNullable(oldestMeta);
    }

    private void deleteFile(String path) throws BackupRestoreException {
        filesToDelete.add(path);
        if (filesToDelete.size() >= BATCH_SIZE) {
            deleteFiles();
            filesToDelete.clear();
        }
    }

    private void deleteFiles() throws BackupRestoreException {
        if (filesToDelete.isEmpty()) {
            return;
        }
        try {
            List<DeleteObjectsRequest.KeyVersion> keys =
                    filesToDelete
                            .stream()
                            .map(DeleteObjectsRequest.KeyVersion::new)
                            .collect(Collectors.toList());
            s3Client.deleteObjects(
                    new DeleteObjectsRequest(getShard()).withKeys(keys).withQuiet(true));
        } catch (Exception e) {
            throw new BackupRestoreException(e + " while trying to delete the objects");
        }
    }

    //TODO implement more completely or deprecate and pass as a parameter
    private String getShard() {
        return "foo";
    }

    // TODO need to re-implement this
    private ImmutableSet<String> getFilesFromMeta(Path localFile) {
        ImmutableSet.Builder<String> files = ImmutableSet.builder();
        try-with-resources
        FileStream.get(localFile)
                .filter(file -> file.contains("backupPath"))
                .forEach(file -> files.add(new String[]{removeCompressionPart(file)}, null);
    }

    static String removeCompressionPart(String backupPath) {
        List<String> parts = Lists.newArrayList(SPLITTER.split(backupPath));
        FileType fileType =
            FileType.valueOf(parts.get(BACKUP_TYPE_INDEX));
        String compressionType;
        if (fileType == FileType.SST_V2) {
            compressionType = parts.remove(7);
        } else if (fileType == FileType.SECONDARY_INDEX_V2) {
            compressionType = parts.remove(8);
        } else {
            throw new IllegalStateException(
                String.format("only %s, and %s, are supported, saw %s",
                    FileType.SST_V2.name(),
                    FileType.SECONDARY_INDEX_V2.name(),
                    fileType));
        }
        // checks compressionType validity
        CompressionType.valueOf(compressionType);
        return JOINER.join(parts);
    }

    // TODO need better name for sstPrefix
    private List<String> getAllMetaFiles(Instant end, String sstPrefix) {
        ArrayList<String> metas = new ArrayList<>();
        String prefix = getMatch(end, sstPrefix);
        String marker = getMatch(null , sstPrefix);
        Iterator<String> iterator = new S3Iterator(s3Client, getShard(), prefix, null, marker);
        while (iterator.hasNext()) {
            String path = iterator.next();
            Instant lastModified = getLastModified(path);
            if (THE_START.compareTo(lastModified) <= 0 && end.compareTo(lastModified) >= 0) {
                metas.add(path);
            }
        }
        // TODO does this have the same effect when sorting Strings?
        metas.sort(Collections.reverseOrder());
        return metas;
    }

    private Instant getLastModified(String backupPath) {
        String lastModified = SPLITTER.limit(LAST_MODIFIED_INDEX + 1).splitToList(backupPath).get(LAST_MODIFIED_INDEX);
        return Instant.ofEpochMilli(Long.parseLong(lastModified));
    }

    private String getMatch(Instant end, String prefix) {
        String match = THE_START.toEpochMilli() + "";
        if (end != null) {
            int diff = StringUtils.indexOfDifference(match, end.toEpochMilli() + "");
            if (diff >= 0) {
                match = match.substring(0, diff);
            }
        }
        return Paths.get(prefix, FileType.META_V2.toString(), match).toString();
    }

    private String prependHash(String appName) {
        return String.format("%d_%s", appName.hashCode() % 10000, appName);
    }

    public Path downloadFile(final String path, final int retry)
            throws BackupRestoreException {
        try {
            new BoundedExponentialRetryCallable<Void>(500, 10000, retry) {
                @Override
                public Void retriableCall() throws Exception {
                    downloadFileImpl(path);
                    return null;
                }
            }.call();
        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage());
        }
        return new File(path.newRestoreFile().getAbsolutePath() + suffix).toPath();
    }

    private void downloadFileImpl(String path)
            throws BackupRestoreException {
        // TODO generate temporary place for meta files to go
        File localFile = new File(path.newRestoreFile().getAbsolutePath() + suffix);
        long size = super.getFileSize(remotePath);
        final int bufferSize = Math.toIntExact(Math.min(MAX_BUFFER_SIZE, size));
        try (BufferedInputStream is =
                     new BufferedInputStream(
                             new RangeReadInputStream(s3Client, getShard(), size, path),
                             bufferSize);
             BufferedOutputStream os =
                     new BufferedOutputStream(new FileOutputStream(localFile))) {
            if (path.getCompression() == CompressionType.NONE) {
                IOUtils.copyLarge(is, os);
            } else {
                compress.decompressAndClose(is, os);
            }
        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage());
        }
    }

    private enum FileType {
        META_V2,
        SECONDARY_INDEX_V2,
        SST_V2;
    }
}
