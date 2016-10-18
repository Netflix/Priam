package com.netflix.priam.merics;

/**
 *
 * A dummy, no op measurement.
 *
 * Created by vinhn on 10/14/16.
 */
public class NoOpMeasurement implements IMeasurement{
    private int failure = 0, success = 0;

    @Override
    public MMEASUREMENT_TYPE getType() {
        return MMEASUREMENT_TYPE.NOOP;
    }

    @Override
    public void incrementFailureCnt(int i) {
        this.failure = i;
    }

    @Override
    public int getFailureCnt() {
        return this.failure;
    }

    @Override
    public void incrementSuccessCnt(int i) {
        this.success = i;
    }

    @Override
    public int getSuccessCnt() {
        return this.success;
    }
}
