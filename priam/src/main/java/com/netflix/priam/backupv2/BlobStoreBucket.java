package com.netflix.priam.backupv2;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

public interface BlobStoreBucket {
    // restoration, validation, deletion
    InputStream get(final String path);
    // Validation, deletion, restoration
    Iterator<String> list(String prefix);
    // deletion
    Iterator<String> commonPrefixes(String prefix, String delimiter, String startAfter);
    // deletion
    void delete(@Nonnull List<String> keys);
    // deletion
    String getName();
    // backup
    ListenableFuture<AbstractBackupPath> putAndDeleteLocalFile(
            final Path path, Instant target, boolean async)
            throws FileNotFoundException, RejectedExecutionException, BackupRestoreException;
    // Backup
    long sizeOf(String remotePath) throws BackupRestoreException;
    // backup
    boolean head(String remotePath);
}