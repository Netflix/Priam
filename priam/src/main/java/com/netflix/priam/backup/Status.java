package com.netflix.priam.backup;

/**
 * Enum to describe the status of the snapshot/restore.
 */
public enum Status {
    /**
     * Denotes snapshot/restore has started successfully and is running.
     */
    STARTED,
    /**
     * Denotes snapshot/restore has finished successfully.
     */
    FINISHED,
    /**
     * Denotes snapshot/restore has failed to upload/restore successfully or there was a failure marking the snapshot/restore as failure.
     */
    FAILED
}
