/*
 * Copyright 2018 Netflix, Inc.
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