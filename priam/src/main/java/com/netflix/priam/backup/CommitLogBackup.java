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

import com.google.common.collect.Lists;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Providing this if we want to use it outside Quart
public class CommitLogBackup {
    private static final Logger logger = LoggerFactory.getLogger(CommitLogBackup.class);
    private final Provider<AbstractBackupPath> pathFactory;
    private final List<String> clRemotePaths = Lists.newArrayList();
    private final IBackupFileSystem fs;

    @Inject
    public CommitLogBackup(
            Provider<AbstractBackupPath> pathFactory, @Named("backup") IBackupFileSystem fs) {
        this.pathFactory = pathFactory;
        this.fs = fs;
    }

    public List<AbstractBackupPath> upload(String archivedDir, final String snapshotName)
            throws Exception {
        logger.info("Inside upload CommitLog files");

        if (StringUtils.isBlank(archivedDir)) {
            throw new IllegalArgumentException("The archived commitlog director is blank or null");
        }

        File archivedCommitLogDir = new File(archivedDir);
        if (!archivedCommitLogDir.exists()) {
            throw new IllegalArgumentException(
                    "The archived commitlog director does not exist: " + archivedDir);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Scanning for backup in: {}", archivedCommitLogDir.getAbsolutePath());
        }
        List<AbstractBackupPath> bps = Lists.newArrayList();
        for (final File file : archivedCommitLogDir.listFiles()) {
            logger.debug("Uploading commit log {} for backup", file.getCanonicalFile());
            try {
                AbstractBackupPath bp = pathFactory.get();
                bp.parseLocal(file, BackupFileType.CL);

                if (snapshotName != null) bp.time = DateUtil.getDate(snapshotName);

                fs.uploadAndDelete(bp, false /* async */);
                bps.add(bp);
                addToRemotePath(bp.getRemotePath());
            } catch (Exception e) {
                logger.error(
                        "Failed to upload local file {}. Ignoring to continue with rest of backup.",
                        file,
                        e);
            }
        }
        return bps;
    }

    private void addToRemotePath(String remotePath) {
        this.clRemotePaths.add(remotePath);
    }
}
