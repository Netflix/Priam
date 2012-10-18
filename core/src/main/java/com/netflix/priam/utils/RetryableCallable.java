package com.netflix.priam.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

public abstract class RetryableCallable<T> implements Callable<T> {
    private static final Logger logger = LoggerFactory.getLogger(RetryableCallable.class);
    public static final int DEFAULT_NUMBER_OF_RETRIES = 15;
    public static final long DEFAULT_WAIT_TIME = 100;
    private int retries;
    private long waitTime;
    private boolean logErrors = true;

    public RetryableCallable() {
        this(DEFAULT_NUMBER_OF_RETRIES, DEFAULT_WAIT_TIME);
    }

    public RetryableCallable(int retries, long waitTime) {
        this.retries = retries;
        this.waitTime = waitTime;
    }

    public RetryableCallable(boolean logErrors) {
        this(DEFAULT_NUMBER_OF_RETRIES, DEFAULT_WAIT_TIME);

        this.logErrors = logErrors;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    public abstract T retriableCall() throws Exception;

    public T call() throws Exception {
        int retry = 0;
        while (true) {
            try {
                return retriableCall();
            } catch (CancellationException e) {
                throw e;
            } catch (Exception e) {
                retry++;
                if (retry == retries) {
                    throw e;
                }
                if (logErrors) {
                    logger.error(String.format("Retry #%d for: %s", retry, e.getMessage()));
                }
                Thread.sleep(waitTime);
            } finally {
                forEachExecution();
            }
        }
    }

    public void forEachExecution() {
        // do nothing by default.
    }
}