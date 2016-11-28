package com.netflix.priam.merics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vinhn on 11/12/16.
 */
public class AWSSlowDownExceptionMeasurement implements IMeasurement<Object> {
    private static final Logger logger = LoggerFactory.getLogger(AWSSlowDownExceptionMeasurement.class);
    private int awsSlowDownExceptionCounter = 0;

    @Override
    public MMEASUREMENT_TYPE getType() {
        return MMEASUREMENT_TYPE.AWSSLOWDOWNEXCEPTION;
    }

    @Override
    public void incrementFailureCnt(int i) {
        this.awsSlowDownExceptionCounter += i;
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
