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
package com.netflix.priam.backupv2;

import com.netflix.priam.compress.CompressionAlgorithm;
import com.netflix.priam.cryptography.CryptographyAlgorithm;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.nio.file.Path;
import java.time.Instant;

/**
 * This is a POJO that will encapsulate the result of file upload. Created by aagrawal on 6/20/18.
 */
public class FileUploadResult {
    private final Path fileName;
    private final Instant lastModifiedTime;
    private final Instant fileCreationTime;
    private final long fileSizeOnDisk; // Size on disk in bytes
    // Valid compression technique for now is SNAPPY only. Future we need to support LZ4 and NONE
    private final CompressionAlgorithm compression = CompressionAlgorithm.SNAPPY;
    // Valid encryption technique for now is PLAINTEXT only. In future we will support pgp and more.
    private final CryptographyAlgorithm encryption = CryptographyAlgorithm.PLAINTEXT;

    private Boolean isUploaded;
    private String backupPath;

    public FileUploadResult(
            Path fileName,
            Instant lastModifiedTime,
            Instant fileCreationTime,
            long fileSizeOnDisk) {
        this.fileName = fileName;
        this.lastModifiedTime = lastModifiedTime;
        this.fileCreationTime = fileCreationTime;
        this.fileSizeOnDisk = fileSizeOnDisk;
    }

    public void setUploaded(Boolean uploaded) {
        isUploaded = uploaded;
    }

    public String getBackupPath() {
        return backupPath;
    }

    public void setBackupPath(String backupPath) {
        this.backupPath = backupPath;
    }

    @Override
    public String toString() {
        return GsonJsonSerializer.getGson().toJson(this);
    }
}
