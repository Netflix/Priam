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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

public abstract class BackupFileSystemAdapter implements IBackupFileSystem{

    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException {
	}

    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException {
	}

    public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
		return null;
	}
    
    public Iterator<AbstractBackupPath> listPrefixes(Date date) {
		return null;
	}

    public void cleanup() {
	}
    
    public int getActivecount() {
		return 0;
	}

    public void shutdown() {
	}
}
