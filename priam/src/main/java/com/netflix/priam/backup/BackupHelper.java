package com.netflix.priam.backup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.ImplementedBy;
import java.io.File;
import java.io.IOException;
import java.time.Instant;

@ImplementedBy(BackupHelperImpl.class)
public interface BackupHelper {

    default ImmutableList<ListenableFuture<AbstractBackupPath>> uploadAndDeleteAllFiles(
            final File parent, final AbstractBackupPath.BackupFileType type, boolean async)
            throws Exception {
        return uploadAndDeleteAllFiles(parent, type, Instant.EPOCH, async);
    }

    ImmutableList<ListenableFuture<AbstractBackupPath>> uploadAndDeleteAllFiles(
            final File parent,
            final AbstractBackupPath.BackupFileType type,
            Instant target,
            boolean async)
            throws Exception;

    ImmutableSet<AbstractBackupPath> getBackupPaths(
            File dir, AbstractBackupPath.BackupFileType type) throws IOException;
}
