/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.netflix.priam.utils.DateUtil;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by aagrawal on 1/31/17.
 */

/*
 * Encapsulates metadata for a snapshot.
 */
public class BackupMetadata implements Serializable {
    private String snapshotDate;
    private String token;
    private Date start, completed;
    private Status status;
    private String snapshotLocation;

    enum Status {STARTED, FINISHED, FAILED}

    public BackupMetadata(String token, Date start) {
        this.snapshotDate = DateUtil.formatyyyyMMdd(start);
        this.token = token;
        this.start = start;
        this.status = Status.STARTED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        BackupMetadata that = (BackupMetadata) o;

        if (!this.snapshotDate.equals(that.snapshotDate)) return false;
        if (!this.token.equals(that.token)) return false;
        return this.start.equals(that.start);
    }

    @Override
    public int hashCode() {
        int result = this.snapshotDate.hashCode();
        result = 31 * result + this.token.hashCode();
        result = 31 * result + this.start.hashCode();
        return result;
    }

    public String getSnapshotDate() {
        return this.snapshotDate;
    }

    public String getToken() {
        return this.token;
    }

    public Date getStart() {
        return this.start;
    }

    public Date getCompleted() {
        return this.completed;
    }

    public BackupMetadata.Status getStatus() {
        return this.status;
    }

    public void setCompleted(Date completed) {
        this.completed = completed;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getSnapshotLocation() {
        return this.snapshotLocation;
    }

    public void setSnapshotLocation(String snapshotLocation) {
        this.snapshotLocation = snapshotLocation;
    }

    public static BackupMetadata clone(BackupMetadata backupMetadata) {
        BackupMetadata metadata = new BackupMetadata(backupMetadata.token, backupMetadata.start);
        metadata.setStatus(backupMetadata.getStatus());
        metadata.setCompleted(backupMetadata.getCompleted());
        return metadata;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BackupMetadata{");
        sb.append("snapshotDate='").append(snapshotDate).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", start=").append(start);
        sb.append(", completed=").append(completed);
        sb.append(", status=").append(status);
        sb.append(", snapshotLocation=").append(snapshotLocation);
        sb.append('}');
        return sb.toString();
    }
}
