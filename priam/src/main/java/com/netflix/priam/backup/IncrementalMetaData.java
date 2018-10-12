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
import com.google.inject.Provider;
import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

public class IncrementalMetaData extends MetaData {

    private String metaFileName = null; // format meta_cf_time (e.g.

    @Inject
    public IncrementalMetaData(
            IConfiguration config,
            Provider<AbstractBackupPath> pathFactory,
            IFileSystemContext backupFileSystemCtx) {
        super(pathFactory, backupFileSystemCtx, config);
    }

    public void setMetaFileName(String name) {
        this.metaFileName = name;
    }

    @Override
    public File createTmpMetaFile() throws IOException {
        File metafile, destFile;

        if (this.metaFileName == null) {

            metafile = File.createTempFile("incrementalMeta", ".json");
            destFile = new File(metafile.getParent(), "incrementalMeta.json");

        } else {
            metafile = File.createTempFile(this.metaFileName, ".json");
            destFile = new File(metafile.getParent(), this.metaFileName + ".json");
        }

        if (destFile.exists()) destFile.delete();

        try {

            FileUtils.moveFile(metafile, destFile);

        } finally {
            if (metafile != null && metafile.exists()) { // clean up resource
                FileUtils.deleteQuietly(metafile);
            }
        }
        return destFile;
    }
}
