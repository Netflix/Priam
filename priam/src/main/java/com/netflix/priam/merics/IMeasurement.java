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
 * Represents a specific measurement for publishing to a metric system
 *
 * Created by vinhn on 10/14/16.
 */
public interface IMeasurement<T> {

    public MMEASUREMENT_TYPE getType();
    public void incrementFailureCnt(int i);
    public int getFailureCnt();
    public void incrementSuccessCnt(int i);
    public int getSuccessCnt();
    /*
    @return a user defined representation of a valuue.
     */
    public T getVal();
    /*
    @param a user defined representation of what you think is a value.
     */
    public void setVal(T val);

    public enum MMEASUREMENT_TYPE {
        NOOP, NODETOOLFLUSH, SNAPSHOTBACKUP, BACKUPUPLOADRATE
        , SNAPSHOTBACKUPUPNOTIFICATION
        , AWSSLOWDOWNEXCEPTION
        ;
    };
}
