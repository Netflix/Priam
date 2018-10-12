/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.priam.config.IConfiguration;

public class BackupFileSystemContext implements IFileSystemContext {

    private IBackupFileSystem fs = null, encryptedFs = null;

    @Inject
    public BackupFileSystemContext(
            @Named("backup") IBackupFileSystem fs,
            @Named("encryptedbackup") IBackupFileSystem encryptedFs) {

        this.fs = fs;
        this.encryptedFs = encryptedFs;
    }

    public IBackupFileSystem getFileStrategy(IConfiguration config) {

        if (!config.isEncryptBackupEnabled()) {

            return this.fs;

        } else {

            return this.encryptedFs;
        }
    }
}
