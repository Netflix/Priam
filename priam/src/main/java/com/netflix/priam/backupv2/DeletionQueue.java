package com.netflix.priam.backupv2;

public interface DeletionQueue {
    void add(String path) throws BackupDeletionException;
    void flush() throws BackupDeletionException;
    int reset();
}
