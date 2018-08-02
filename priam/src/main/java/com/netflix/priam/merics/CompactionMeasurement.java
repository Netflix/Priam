package com.netflix.priam.merics;

/**
 * Measurement class for scheduled compactions
 * Created by aagrawal on 2/28/18.
 */

public class CompactionMeasurement implements IMeasurement<Object> {
    private int failure = 0, success = 0;

    @Override
    public MMEASUREMENT_TYPE getType() {
        return MMEASUREMENT_TYPE.COMPACTION;
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

    @Override
    public Object getVal() {
        return null;
    }

    @Override
    public void setVal(Object val) {
        //NO op;
    }

}