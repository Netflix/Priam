package com.netflix.priam.merics;

/**
 * Created by vinhn on 10/14/16.
 */
public class SnapshotBackupMeasurement  implements IMeasurement  {
    private int failure = 0, success = 0;
    @Override
    public MMEASUREMENT_TYPE getType() {
        return MMEASUREMENT_TYPE.SNAPSHOTBACKUP;
    }

    public void incrementFailureCnt(int val) {
        this.failure += val;
    }
    public int getFailureCnt() {
        return this.failure;
    }

    public void incrementSuccessCnt(int val) {
        this.success += val;
    }
    public int getSuccessCnt() {
        return this.success;
    }
}
