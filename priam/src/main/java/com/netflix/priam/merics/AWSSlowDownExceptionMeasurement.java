package com.netflix.priam.merics;

/**
 * Created by vinhn on 11/12/16.
 */
public class AWSSlowDownExceptionMeasurement implements IMeasurement<Object> {
    private int awsSlowDownExceptionCounter = 0;

    @Override
    public MMEASUREMENT_TYPE getType() {
        return MMEASUREMENT_TYPE.AWSSLOWDOWNEXCEPTION;
    }

    @Override
    public void incrementFailureCnt(int i) {
        this.awsSlowDownExceptionCounter += 1;
    }

    @Override
    public int getFailureCnt() {
        return this.awsSlowDownExceptionCounter;
    }

    @Override
    public void incrementSuccessCnt(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSuccessCnt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getVal() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setVal(Object val) {
        throw new UnsupportedOperationException();
    }
}
