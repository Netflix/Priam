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

import com.google.inject.ImplementedBy;
import com.netflix.priam.merics.BackupMetricsMgr;

/**
 * A means to keep track of various metata about backups
 * Created by vinhn on 2/13/17.
 */
@ImplementedBy(BackupMetricsMgr.class)
public interface IBackupMetrics {
    int getValidUploads();

    void incrementValidUploads();

    int getInvalidUploads();  //defers the semantic of "invalid upload" to implementation

    void incrementInvalidUploads();

    int getValidDownloads();

    void incrementValidDownloads();

    int getInvalidDownloads();

    void incrementInvalidDownloads();
}
