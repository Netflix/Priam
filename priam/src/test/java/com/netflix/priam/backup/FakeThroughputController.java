package com.netflix.priam.backup;

import java.time.Instant;

public class FakeThroughputController implements ThroughputController {
    @Override
    public double getDesiredThroughput(AbstractBackupPath dir, Instant target) {
        return Double.MAX_VALUE;
    }
}
