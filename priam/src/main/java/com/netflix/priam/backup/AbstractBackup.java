/*
 * Copyright 2013 Netflix, Inc.
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

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.SystemUtils;
import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract Backup class for uploading files to backup location */
public abstract class AbstractBackup extends Task {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBackup.class);
    static final String INCREMENTAL_BACKUP_FOLDER = "backups";
    public static final String SNAPSHOT_FOLDER = "snapshots";

    @Inject
    public AbstractBackup(IConfiguration config) {
        super(config);
    }

    protected final void initiateBackup(
            String monitoringFolder, BackupRestoreUtil backupRestoreUtil) throws Exception {

        File dataDir = new File(config.getDataFileLocation());
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            throw new IllegalArgumentException(
                    "The configured 'data file location' does not exist or is not a directory: "
                            + config.getDataFileLocation());
        }
        logger.debug("Scanning for backup in: {}", dataDir.getAbsolutePath());
        File[] keyspaceDirectories = dataDir.listFiles();
        if (keyspaceDirectories == null) return;

        for (File keyspaceDir : keyspaceDirectories) {
            if (keyspaceDir.isFile()) continue;

            logger.debug("Entering {} keyspace..", keyspaceDir.getName());
            File[] columnFamilyDirectories = keyspaceDir.listFiles();
            if (columnFamilyDirectories == null) continue;

            for (File columnFamilyDir : columnFamilyDirectories) {
                File backupDir = new File(columnFamilyDir, monitoringFolder);
                if (isAReadableDirectory(backupDir)) {
                    String columnFamilyName = getColumnFamily(backupDir);
                    if (backupRestoreUtil.isFiltered(keyspaceDir.getName(), columnFamilyName)) {
                        // Clean the backup/snapshot directory else files will keep getting
                        // accumulated.
                        SystemUtils.cleanupDir(backupDir.getAbsolutePath(), null);
                    } else {
                        processColumnFamily(backupDir);
                    }
                }
            } // end processing all CFs for keyspace
        } // end processing keyspaces under the C* data dir
    }

    protected String getColumnFamily(File backupDir) {
        return backupDir.getParentFile().getName().split("-")[0];
    }

    protected String getKeyspace(File backupDir) {
        return backupDir.getParentFile().getParentFile().getName();
    }

    /**
     * Process the columnfamily in a given snapshot/backup directory.
     *
     * @param backupDir Location of the backup/snapshot directory in that columnfamily.
     * @throws Exception throws exception if there is any error in process the directory.
     */
    protected abstract void processColumnFamily(File backupDir) throws Exception;

    /**
     * Get all the backup directories for Cassandra.
     *
     * @param config to get the location of the data folder.
     * @param monitoringFolder folder where cassandra backup's are configured.
     * @return Set of the path(s) containing the backup folder for each columnfamily.
     * @throws Exception incase of IOException.
     */
    public static Set<Path> getBackupDirectories(IConfiguration config, String monitoringFolder)
            throws Exception {
        HashSet<Path> backupPaths = new HashSet<>();
        if (config.getDataFileLocation() == null) return backupPaths;
        Path dataPath = Paths.get(config.getDataFileLocation());
        if (Files.exists(dataPath) && Files.isDirectory(dataPath))
            try (DirectoryStream<Path> directoryStream =
                    Files.newDirectoryStream(dataPath, path -> Files.isDirectory(path))) {
                for (Path keyspaceDirPath : directoryStream) {
                    try (DirectoryStream<Path> keyspaceStream =
                            Files.newDirectoryStream(
                                    keyspaceDirPath, path -> Files.isDirectory(path))) {
                        for (Path columnfamilyDirPath : keyspaceStream) {
                            Path backupDirPath =
                                    Paths.get(columnfamilyDirPath.toString(), monitoringFolder);
                            if (Files.exists(backupDirPath) && Files.isDirectory(backupDirPath)) {
                                logger.debug("Backup folder: {}", backupDirPath);
                                backupPaths.add(backupDirPath);
                            }
                        }
                    }
                }
            }
        return backupPaths;
    }

    protected static boolean isAReadableDirectory(File dir) {
        return dir.exists() && dir.isDirectory() && dir.canRead();
    }
}
