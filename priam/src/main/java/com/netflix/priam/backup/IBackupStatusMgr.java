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
package com.netflix.priam.backup;

import com.google.inject.ImplementedBy;


import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by aagrawal on 1/30/17.
 */
@ImplementedBy(FileSnapshotStatusMgr.class)
public interface IBackupStatusMgr {
    List<BackupMetadata> locate(Date snapshotDate);
    List<BackupMetadata> locate(String snapshotDate);
    void start(BackupMetadata backupMetadata);
    void finish(BackupMetadata backupMetadata);
    void failed(BackupMetadata backupMetadata);
    int getCapacity();
    Map<String, LinkedList<BackupMetadata>> getAllSnapshotStatus();
}
