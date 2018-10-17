/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RetryableCallable<T> implements Callable<T> {
    private static final Logger logger = LoggerFactory.getLogger(RetryableCallable.class);
    private static final int DEFAULT_NUMBER_OF_RETRIES = 15;
    private static final long DEFAULT_WAIT_TIME = 100;
    private int retrys;
    private long waitTime;

    public RetryableCallable() {
        this(DEFAULT_NUMBER_OF_RETRIES, DEFAULT_WAIT_TIME);
    }

    public RetryableCallable(int retrys, long waitTime) {
        set(retrys, waitTime);
    }

    public void set(int retrys, long waitTime) {
        this.retrys = retrys;
        this.waitTime = waitTime;
    }

    protected abstract T retriableCall() throws Exception;

    public T call() throws Exception {
        int retry = 0;
        int logCounter = 0;
        while (true) {
            try {
                return retriableCall();
            } catch (CancellationException e) {
                throw e;
            } catch (Exception e) {
                retry++;
                if (retry == retrys) {
                    throw e;
                }
                logger.error("Retry #{} for: {}", retry, e.getMessage());

                if (++logCounter == 1 && logger.isErrorEnabled())
                    logger.error("Exception --> " + ExceptionUtils.getStackTrace(e));
                Thread.sleep(waitTime);
            } finally {
                forEachExecution();
            }
        }
    }

    protected void forEachExecution() {
        // do nothing by default.
    }
}
