package com.netflix.priam.backup;

import static java.util.stream.Collectors.toSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.config.BackupsToCompress;
import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Provider;

public class BackupHelperImpl implements BackupHelper {

    private static final String COMPRESSION_SUFFIX = "-CompressionInfo.db";
    private static final String DATA_SUFFIX = "-Data.db";
    private final Provider<AbstractBackupPath> pathFactory;
    private final IBackupFileSystem fs;
    private final IConfiguration config;

    @Inject
    public BackupHelperImpl(
            IConfiguration config,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> pathFactory) {
        this.config = config;
        this.pathFactory = pathFactory;
        this.fs = backupFileSystemCtx.getFileStrategy(config);
    }

    /**
     * Upload files in the specified dir. Does not delete the file in case of error. The files are
     * uploaded serially or async based on flag provided.
     *
     * @param parent Parent dir
     * @param type Type of file (META, SST, SNAP etc)
     * @param async Upload the file(s) in async fashion if enabled.
     * @return List of files that are successfully uploaded as part of backup
     * @throws Exception when there is failure in uploading files.
     */
    @Override
    public ImmutableList<ListenableFuture<AbstractBackupPath>> uploadAndDeleteAllFiles(
            final File parent,
            final AbstractBackupPath.BackupFileType type,
            Instant target,
            boolean async)
            throws Exception {
        final ImmutableList.Builder<ListenableFuture<AbstractBackupPath>> futures =
                ImmutableList.builder();
        for (AbstractBackupPath bp : getBackupPaths(parent, type)) {
            futures.add(fs.uploadAndDelete(bp, target, async));
        }
        return futures.build();
    }

    @Override
    public ImmutableSet<AbstractBackupPath> getBackupPaths(
            File dir, AbstractBackupPath.BackupFileType type) throws IOException {
        Set<File> files;
        try (Stream<Path> pathStream = Files.list(dir.toPath())) {
            files = pathStream.map(Path::toFile).filter(File::isFile).collect(toSet());
        }
        Set<String> compressedFilePrefixes =
                files.stream()
                        .map(File::getName)
                        .filter(name -> name.endsWith(COMPRESSION_SUFFIX))
                        .map(name -> name.substring(0, name.lastIndexOf('-')))
                        .collect(toSet());
        final ImmutableSet.Builder<AbstractBackupPath> bps = ImmutableSet.builder();
        ImmutableSet.Builder<AbstractBackupPath> dataFiles = ImmutableSet.builder();
        for (File file : files) {
            final AbstractBackupPath bp = pathFactory.get();
            bp.parseLocal(file, type);
            bp.setCompression(getCorrectCompressionAlgorithm(bp, compressedFilePrefixes));
            (file.getAbsolutePath().endsWith(DATA_SUFFIX) ? dataFiles : bps).add(bp);
        }
        bps.addAll(dataFiles.build());
        return bps.build();
    }

    private CompressionType getCorrectCompressionAlgorithm(
            AbstractBackupPath path, Set<String> compressedFiles) {
        if (!AbstractBackupPath.BackupFileType.isV2(path.getType())
                || path.getLastModified().toEpochMilli()
                        < config.getCompressionTransitionEpochMillis()) {
            return CompressionType.SNAPPY;
        }
        String file = path.getFileName();
        BackupsToCompress which = config.getBackupsToCompress();
        switch (which) {
            case NONE:
                return CompressionType.NONE;
            case ALL:
                return CompressionType.SNAPPY;
            case IF_REQUIRED:
                int splitIndex = file.lastIndexOf('-');
                return splitIndex >= 0 && compressedFiles.contains(file.substring(0, splitIndex))
                        ? CompressionType.NONE
                        : CompressionType.SNAPPY;
            default:
                throw new IllegalArgumentException("NONE, ALL, UNCOMPRESSED only. Saw: " + which);
        }
    }
}
