/*
 * Copyright 2019 Netflix, Inc.
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

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 1/1/19. */
public class ForgottenFilesManager {
    private static final Logger logger = LoggerFactory.getLogger(ForgottenFilesManager.class);

    private BackupMetrics backupMetrics;
    private IConfiguration config;
    private static final String TMP_EXT = ".tmp";

    private static final Pattern tmpFilePattern =
            Pattern.compile("^((.*)\\-(.*)\\-)?tmp(link)?\\-((?:l|k).)\\-(\\d)*\\-(.*)$");

    protected static final String LOST_FOUND = "lost+found";

    @Inject
    public ForgottenFilesManager(IConfiguration configuration, BackupMetrics backupMetrics) {
        this.config = configuration;
        this.backupMetrics = backupMetrics;
    }

    public void findAndMoveForgottenFiles(Instant snapshotInstant, File snapshotDir) {
        try {
            Collection<File> snapshotFiles =
                    FileUtils.listFiles(snapshotDir, FileFilterUtils.fileFileFilter(), null);
            File columnfamilyDir = snapshotDir.getParentFile().getParentFile();
            Collection<File> columnfamilyFiles =
                    getColumnfamilyFiles(snapshotInstant, columnfamilyDir);

            // Remove the SSTable(s) which are part of snapshot from the CF file list.
            // This cannot be a simple removeAll as snapshot files have "different" file folder
            // prefix.
            for (File file : snapshotFiles) {
                // Get its parent directory file based on this file.
                File originalFile = new File(columnfamilyDir, file.getName());
                columnfamilyFiles.remove(originalFile);
            }

            // If there are no "extra" SSTables in CF data folder, we are done.
            if (columnfamilyFiles.size() == 0) return;

            logger.warn(
                    "# of potential forgotten files: {} found for CF: {}",
                    columnfamilyFiles.size(),
                    columnfamilyDir.getName());

            // Move the files to lost_found directory if configured.
            moveForgottenFiles(columnfamilyDir, columnfamilyFiles);

        } catch (Exception e) {
            // Eat the exception, if there, for any reason. This should not stop the snapshot for
            // any reason.
            logger.error(
                    "Exception occurred while trying to find forgottenFile. Ignoring the error and continuing with remaining backup",
                    e);
            e.printStackTrace();
        }
    }

    protected Collection<File> getColumnfamilyFiles(Instant snapshotInstant, File columnfamilyDir) {
        // Find all the files in columnfamily folder which is :
        // 1. Not a temp file.
        // 2. Is a file. (we don't care about directories)
        // 3. Is older than snapshot time, as new files keep getting created after taking a
        // snapshot.
        IOFileFilter tmpFileFilter1 = FileFilterUtils.suffixFileFilter(TMP_EXT);
        IOFileFilter tmpFileFilter2 =
                FileFilterUtils.asFileFilter(
                        pathname -> tmpFilePattern.matcher(pathname.getName()).matches());
        IOFileFilter tmpFileFilter = FileFilterUtils.or(tmpFileFilter1, tmpFileFilter2);
        /*
        Here we are allowing files which were more than
        @link{IConfiguration#getGracePeriodDaysForCompaction}. We do this to allow cassandra
        to have files which were generated as part of long running compaction.
        Refer to https://issues.apache.org/jira/browse/CASSANDRA-6756 and
        https://issues.apache.org/jira/browse/CASSANDRA-7066
        for more information.
        */
        IOFileFilter ageFilter =
                FileFilterUtils.ageFileFilter(
                        snapshotInstant
                                .minus(config.getGracePeriodDaysForCompaction(), ChronoUnit.DAYS)
                                .toEpochMilli());
        IOFileFilter fileFilter =
                FileFilterUtils.and(
                        FileFilterUtils.notFileFilter(tmpFileFilter),
                        FileFilterUtils.fileFileFilter(),
                        ageFilter);

        return FileUtils.listFiles(columnfamilyDir, fileFilter, null);
    }

    protected void moveForgottenFiles(File columnfamilyDir, Collection<File> columnfamilyFiles)
            throws IOException {
        // This is a list of potential forgotten file(s). Note that C* might still be using
        // files as part of read, so we really do not want to move them until we meet the
        // @link{IConfiguration#getForgottenFileGracePeriodDaysForRead} window elapses.

        final Path destDir = Paths.get(columnfamilyDir.getAbsolutePath(), LOST_FOUND);
        FileUtils.forceMkdir(destDir.toFile());
        final Collection<Path> columnfamilyPaths =
                columnfamilyFiles
                        .parallelStream()
                        .map(file -> Paths.get(file.getAbsolutePath()))
                        .collect(Collectors.toList());

        for (Path file : columnfamilyPaths) {
            try {
                final Path symbolic_link =
                        Paths.get(destDir.toFile().getAbsolutePath(), file.toFile().getName());
                // Lets see if there is a symbolic link to this file already?
                if (!Files.exists(symbolic_link)) {
                    // If not, lets create one and work on next file.
                    Files.createSymbolicLink(symbolic_link, file);
                    continue;
                } else if (Files.isSymbolicLink(symbolic_link)) {
                    // Symbolic link exists, is it older than our timeframe?
                    Instant last_modified_time =
                            Files.getLastModifiedTime(symbolic_link, LinkOption.NOFOLLOW_LINKS)
                                    .toInstant();
                    if (DateUtil.getInstant()
                            .isAfter(
                                    last_modified_time.plus(
                                            config.getForgottenFileGracePeriodDaysForRead(),
                                            ChronoUnit.DAYS))) {
                        // Eligible for move.
                        logger.info(
                                "Eligible for move: Forgotten file: {} found for CF: {}",
                                file,
                                columnfamilyDir.getName());
                        backupMetrics.incrementForgottenFiles(1);
                        if (config.isForgottenFileMoveEnabled()) {
                            try {
                                // Remove our symbolic link. Note that deletion of symbolic link
                                // does not remove the original file.
                                Files.delete(symbolic_link);
                                FileUtils.moveFileToDirectory(
                                        file.toFile(), destDir.toFile(), true);
                                logger.warn(
                                        "Successfully moved forgotten file: {} found for CF: {}",
                                        file,
                                        columnfamilyDir.getName());
                            } catch (IOException e) {
                                logger.error(
                                        "Exception occurred while trying to move forgottenFile: {}. Ignoring the error and continuing with remaining backup/forgotten files.",
                                        file);
                                e.printStackTrace();
                            }
                        }
                    }
                }

            } catch (IOException e) {
                logger.error("Forgotten file: Error while trying to process the file: {}", file);
                e.printStackTrace();
            }
        }

        // Clean LOST_FOUND directory of any previous symbolic link files which are not considered
        // lost any more.
        for (File file : FileUtils.listFiles(destDir.toFile(), null, false)) {
            Path filePath = Paths.get(file.getAbsolutePath());
            if (Files.isSymbolicLink(filePath)) {
                Path originalFile = Files.readSymbolicLink(filePath);
                if (!columnfamilyPaths.contains(originalFile)) {
                    Files.delete(filePath);
                    logger.info(
                            "Deleting the symbolic link as it is not considered as lost anymore. filePath: {}",
                            filePath);
                }
            }
        }
    }
}
