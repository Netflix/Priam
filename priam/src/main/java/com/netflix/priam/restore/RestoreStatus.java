/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.restore;

import com.netflix.priam.backup.Status;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.DateUtil;

import javax.inject.Singleton;
import java.time.LocalDateTime;

/**
 * This is POJO class to hold the status of the restore.
 * Created by aagrawal on 10/1/17.
 */
@Singleton
public class RestoreStatus {
    private LocalDateTime startDateRange, endDateRange; //Date range to restore from
    private LocalDateTime execStartTime, execEndTime; //Start-end time of the actual restore execution
    private String snapshotMetaFile; //Location of the snapshot meta file selected for restore.
    private Status status;  //the state of a restore.  Note: this is different than the "status" of a Task.

    public void resetStatus(){
        this.snapshotMetaFile = null;
        this.status = null;
        this.startDateRange = endDateRange = null;
        this.execStartTime = this.execEndTime = null;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("RestoreStatus{");
        sb.append("startDateRange=").append(startDateRange);
        sb.append(", endDateRange=").append(endDateRange);
        sb.append(", execStartTime=").append(execStartTime);
        sb.append(", execEndTime=").append(execEndTime);
        sb.append(", snapshotMetaFile='").append(snapshotMetaFile).append('\'');
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public void setStartDateRange(LocalDateTime startDateRange) {
        this.startDateRange = startDateRange;
    }

    public void setEndDateRange(LocalDateTime endDateRange) {
        this.endDateRange = endDateRange;
    }

    public void setExecStartTime(LocalDateTime execStartTime) {
        this.execStartTime = execStartTime;
    }

    public void setExecEndTime(LocalDateTime execEndTime) {
        this.execEndTime = execEndTime;
    }

    public LocalDateTime getStartDateRange() {
        return startDateRange;
    }

    public LocalDateTime getEndDateRange() {
        return endDateRange;
    }

    public LocalDateTime getExecStartTime() {
        return execStartTime;
    }

    public LocalDateTime getExecEndTime() {
        return execEndTime;
    }

    public String getSnapshotMetaFile() {
        return snapshotMetaFile;
    }

    public void setSnapshotMetaFile(String snapshotMetaFile) {
        this.snapshotMetaFile = snapshotMetaFile;
    }

    public String toJson(){
        final StringBuffer sb = new StringBuffer("RestoreStatus{");
        sb.append("startDateRange=").append(DateUtil.formatyyyyMMddHHmm(startDateRange));
        sb.append(", endDateRange=").append(DateUtil.formatyyyyMMddHHmm(endDateRange));
        sb.append(", execStartTime=").append(DateUtil.formatyyyyMMddHHmm(execStartTime));
        sb.append(", execEndTime=").append(DateUtil.formatyyyyMMddHHmm(execEndTime));
        sb.append(", snapshotMetaFile='").append(snapshotMetaFile).append('\'');
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }
}
