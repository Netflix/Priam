package com.netflix.priam.backup;

/**
 * A means to keep track of various metata about backups
 * Created by vinhn on 2/13/17.
 */
public interface IBackupMetrics {
    public int getValidUploads();
    public void incrementValidUploads();
    public int getInvalidUploads();  //defers the semantic of "invalid upload" to implementation
    public void incrementInvalidUploads();
}
