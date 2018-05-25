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
package com.netflix.priam.backup;

import com.amazonaws.services.devicefarm.model.UploadStatus;

/**
 * Created by aagrawal on 5/16/18.
 */
public class UploadResult {

    private AbstractBackupPath file;
    private UploadStatus uploadStatus = UploadStatus.INITIALIZED;

    public UploadResult(AbstractBackupPath file) {
        this.file = file;
    }

    public AbstractBackupPath getFile() {
        return file;
    }

    public void setFile(AbstractBackupPath file) {
        this.file = file;
    }

    public UploadStatus getUploadStatus() {
        return uploadStatus;
    }

    public void setUploadStatus(UploadStatus uploadStatus) {
        this.uploadStatus = uploadStatus;
    }
}
