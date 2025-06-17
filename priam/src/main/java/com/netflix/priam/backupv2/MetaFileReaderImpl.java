package com.netflix.priam.backupv2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.gson.stream.JsonReader;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import com.netflix.priam.utils.GsonJsonSerializer;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.BufferedOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class MetaFileReaderImpl implements IMetaFileReader {
    private static final String META_FILE_DATA = "data";
    // TODO our LIST calls might be cheaper if this was later. Look into that.
    private static final Instant THE_START = Instant.parse("2018-01-01T00:00:00Z");
    private final BlobStoreBucket bucket;
    private final BackupDeleterConfiguration configuration;
    private final Thrower thrower;

    @Inject
    public MetaFileReaderImpl(
            BlobStoreBucket bucket,
            BackupDeleterConfiguration configuration,
            ThrowerFactory throwerFactory) {
        this.bucket = bucket;
        this.configuration = configuration;
        this.thrower = throwerFactory.create(LoggerFactory.getLogger(MetaFileReaderImpl.class));
    }

    @Override
    public ImmutableSet<String> read(String meta) throws Exception {
        Path metaFileName =  Paths.get(meta).getFileName();
        Path destination = configuration.getLocalMetaFileStagingDirectory().resolve(metaFileName);
        ImmutableSet<String> referenceFiles = ImmutableSet.of();
        try {
            downloadFile(meta, destination, configuration.getMaxMetaFileReadRetries());
            referenceFiles = getFilesFromMeta(destination);
        } catch (Exception e) {
            thrower.abort(Failure.FAILED_READING_META_FILE, String.format("meta file = %s", meta), e);
        }
        if (referenceFiles.isEmpty()) {
            thrower.abort(Failure.EMPTY_META_FILE, String.format("meta file = %s", meta));
        }
        PathUtils.tryDelete(destination);
        return referenceFiles;
    }

    @Override
    public ImmutableList<String> getMetas(String tokenPrefix, @Nonnull Instant end) throws BackupDeletionException {
        String writeTimePrefix = StringUtils.getCommonPrefix(THE_START.toEpochMilli() + "", end.toEpochMilli() + "");
        ImmutableList<String> metas =
                Streams.stream(bucket.list(BackupPaths.JOINER.join(tokenPrefix, FileType.META_V2, writeTimePrefix)))
                        .filter(path -> BackupPaths.getLastModified(path).compareTo(end) <= 0)
                        .sorted(Collections.reverseOrder())
                        .collect(toImmutableList());
        if (metas.isEmpty()) {
            thrower.abort(Failure.NO_USABLE_META_FILE, String.format("tokenPrefix: %s, now:  %s", tokenPrefix, end));
        }
        return metas;
    }

    private ImmutableSet<String> getFilesFromMeta(@Nonnull Path metaFilePath) throws IOException {
        if (!metaFilePath.toFile().exists() || !metaFilePath.toFile().isFile()) {
            throw new IllegalArgumentException(String.format("File not found: %s", metaFilePath));
        }
        ImmutableSet.Builder<String> files = ImmutableSet.builder();
        JsonReader jsonReader = new JsonReader(new FileReader(metaFilePath.toFile()));
        jsonReader.beginObject();
        while (jsonReader.hasNext()) {
            if (!jsonReader.nextName().equals(META_FILE_DATA)) {
                continue;
            }
            jsonReader.beginArray();
            while (jsonReader.hasNext()) {
                ColumnFamilyResult columnFamilyResult =
                        GsonJsonSerializer.getGson().fromJson(jsonReader, ColumnFamilyResult.class);
                for (ColumnFamilyResult.SSTableResult ssTableResult : columnFamilyResult.getSstables()) {
                    for (FileUploadResult fileUploadResult : ssTableResult.getSstableComponents()) {
                        files.add(fileUploadResult.getBackupPath());
                    }
                }
            }
            jsonReader.endArray();
        }
        jsonReader.endObject();
        jsonReader.close();
        return files.build();
    }

    private void downloadFile(final String origin, final Path destination, final int retry) throws Exception {
        new BoundedExponentialRetryCallable<Void>(500, 10000, retry) {
            @Override
            public Void retriableCall() throws IOException {
                downloadFileImpl(origin, destination);
                return null;
            }
        }.call();
    }

    private void downloadFileImpl(String origin, Path destination) throws IOException {
        if (BackupPaths.getCompressionType(origin) == CompressionType.NONE) {
            try (InputStream is = bucket.get(origin);
                 BufferedOutputStream os = new BufferedOutputStream(Files.newOutputStream(destination))) {
                IOUtils.copyLarge(is, os);
            }
        } else {
            int bufferSize = 2 << 15;
            byte[] data = new byte[bufferSize];
            try (SnappyInputStream snappyInputStream = new SnappyInputStream(bucket.get(origin));
                 BufferedOutputStream dest = new BufferedOutputStream(Files.newOutputStream(destination), bufferSize)) {
                int c;
                while ((c = snappyInputStream.read(data, 0, bufferSize)) != -1) {
                    dest.write(data, 0, c);
                }
            }
        }
    }
}
