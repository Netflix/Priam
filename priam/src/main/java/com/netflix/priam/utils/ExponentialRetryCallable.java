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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;

public abstract class ExponentialRetryCallable<T> extends RetryableCallable<T> {
    public final static long MAX_SLEEP = 240000;
    public final static long MIN_SLEEP = 200;

    private static final Logger logger = LoggerFactory.getLogger(ExponentialRetryCallable.class);
    private long max;
    private long min;

    public ExponentialRetryCallable() {
        this.max = MAX_SLEEP;
        this.min = MIN_SLEEP;
    }

    public ExponentialRetryCallable(long minSleep, long maxSleep) {
        this.max = maxSleep;
        this.min = minSleep;
    }

    public T call() throws Exception {
        long delay = min;// ms
        while (true) {
            try {
                return retriableCall();
            } catch (CancellationException e) {
                throw e;
            } catch (Exception e) {
                delay *= 2;
                if (delay > max) {
                    throw e;
                }
                logger.error(e.getMessage());
                Thread.sleep(delay);
            } finally {
                forEachExecution();
            }
        }
    }

}