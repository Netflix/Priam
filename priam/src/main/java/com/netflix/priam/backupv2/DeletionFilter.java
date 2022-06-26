package com.netflix.priam.backupv2;

import com.google.inject.ImplementedBy;
import java.time.Instant;

/** Hook to allow forks to optionally omit files from deletion. */
@ImplementedBy(NoOpDeletionFilter.class)
public interface DeletionFilter {
    default boolean shouldDelete(Instant lastModified) {
        return true;
    }
}
