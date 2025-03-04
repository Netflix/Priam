package com.netflix.priam.backupv2;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

public interface BlobStore {
    // restoration, validation, deletion
    Future<Path> get(final AbstractBackupPath path, final int retry)
            throws BackupRestoreException, RejectedExecutionException;
    // Validation, deletion, restoration
    Iterator<String> list(String prefix, String marker);
    // deletion
    void delete(List<String> remotePaths) throws BackupRestoreException;
    // backup
    ListenableFuture<AbstractBackupPath> putAndDeleteLocalFile(
            final Path path, Instant target, boolean async)
            throws FileNotFoundException, RejectedExecutionException, BackupRestoreException;
    // Backup
    long sizeOf(String remotePath) throws BackupRestoreException;
    // backup
    boolean head(String remotePath)
}