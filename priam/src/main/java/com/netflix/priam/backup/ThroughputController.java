package com.netflix.priam.backup;

import com.google.inject.ImplementedBy;
import java.time.Instant;

@ImplementedBy(BackupThroughputController.class)
public interface ThroughputController {
    double getDesiredThroughput(AbstractBackupPath dir, Instant target);
}
