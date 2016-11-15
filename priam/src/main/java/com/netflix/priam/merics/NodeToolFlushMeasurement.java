package com.netflix.priam.merics;

/**
 *
 * Represents the value to be publish to a telemetry endpoint
 *
 * Created by vinhn on 10/14/16.
 */
public class NodeToolFlushMeasurement implements IMeasurement<Object> {
    private int failure = 0, success = 0;

    @Override
    public MMEASUREMENT_TYPE getType() {
        return MMEASUREMENT_TYPE.NODETOOLFLUSH;
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
