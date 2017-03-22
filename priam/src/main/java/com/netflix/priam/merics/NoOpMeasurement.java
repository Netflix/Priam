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

/**
 *
 * A dummy, no op measurement.
 *
 * Created by vinhn on 10/14/16.
 */
public class NoOpMeasurement implements IMeasurement<Object>{
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

    @Override
    public Object getVal() {
        return null;
    }

    @Override
    public void setVal(Object val) {

    }
}
