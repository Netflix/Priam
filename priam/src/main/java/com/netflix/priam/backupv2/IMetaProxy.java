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

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.BackupVerificationResult;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/** Proxy to do management tasks for meta files. Created by aagrawal on 12/18/18. */
public interface IMetaProxy {

    /**
     * Path on the local file system where meta file should be stored for processing.
     *
     * @return location on local file system.
     */
    Path getLocalMetaFileDirectory();

    /**
     * Get the prefix for the manifest file. This will depend on the configuration, if restore
     * prefix is set.
     *
     * @param dateRange date range for which we are trying to find manifest files.
     * @return prefix for the manifest files.
     */
    String getMetaPrefix(DateUtil.DateRange dateRange);

    /**
     * Fetch the list of all manifest files on the remote file system for the provided valid
     * daterange.
     *
     * @param dateRange the time period to scan in the remote file system for meta files.
     * @return List of all the manifest files from the remote file system.
     */
    List<AbstractBackupPath> findMetaFiles(DateUtil.DateRange dateRange);

    /**
     * Download the meta file to disk.
     *
     * @param meta AbstractBackupPath denoting the meta file on remote file system.
     * @return the location of the meta file on disk after downloading from remote file system.
     * @throws BackupRestoreException if unable to download for any reason.
     */
    Path downloadMetaFile(AbstractBackupPath meta) throws BackupRestoreException;

    /**
     * Read the manifest file and give the contents of the file (all the sstable components) as
     * list.
     *
     * @param localMetaPath location of the manifest file on disk.
     * @return list containing all the remote locations of sstable components.
     * @throws Exception if file is not found on local system or is corrupt.
     */
    List<String> getSSTFilesFromMeta(Path localMetaPath) throws Exception;

    /**
     * Get the list of incremental files given the daterange.
     *
     * @param dateRange the time period to scan in the remote file system for incremental files.
     * @return iterator containing the list of path on the remote file system satisfying criteria.
     */
    Iterator<AbstractBackupPath> getIncrementals(DateUtil.DateRange dateRange);

    /**
     * Validate that all the files mentioned in the meta file actually exists on remote file system.
     *
     * @param metaBackupPath Path to the remote meta file.
     * @return backupVerificationResult containing the information like valid - if all the files
     *     mentioned in meta file are present on remote file system. It will return false in case of
     *     any error.
     */
    BackupVerificationResult isMetaFileValid(AbstractBackupPath metaBackupPath);

    /** Delete the old meta files, if any present in the metaFileDirectory */
    void cleanupOldMetaFiles();
}
