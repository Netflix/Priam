/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
