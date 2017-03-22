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
 * Created by vinhn on 10/19/16.
 */
public class BackupUploadRateMeasurement implements IMeasurement<BackupUploadRateMeasurement.Metadata> {

    private BackupUploadRateMeasurement.Metadata metadata;
    private int incrementSuccessCnt = 0;

    @Override
    public MMEASUREMENT_TYPE getType() {
        return IMeasurement.MMEASUREMENT_TYPE.BACKUPUPLOADRATE;
    }

    @Override
    public void incrementFailureCnt(int i) {

        //No op
    }

    @Override
    public int getFailureCnt() {
        return 0;
    }

    @Override
    public void incrementSuccessCnt(int i) {
        this.incrementSuccessCnt += i;
    }

    @Override
    public int getSuccessCnt() {
        return this.incrementSuccessCnt;
    }

    @Override
    public BackupUploadRateMeasurement.Metadata getVal() {
        return this.metadata;
    }

    @Override
    public void setVal(BackupUploadRateMeasurement.Metadata val) {
        this.metadata = val;
    }

    public static class Metadata {
        private final double uploadRateKBps;
        private final long elapseTimeInMillisecs;
        private final String fileName;

        public Metadata(String fileName, double uploadRateKBps, long elapseTimeInMillisecs) {
            this.fileName = fileName;
            this.uploadRateKBps = uploadRateKBps;
            this.elapseTimeInMillisecs = elapseTimeInMillisecs;
        }
        public String getFileName() {
            return this.fileName;
        }
        public double getUploadRateKBps() {
            return this.uploadRateKBps;
        }
        public long getGetElapseTimeInMillisecs() {return this.elapseTimeInMillisecs; }
    }
}
