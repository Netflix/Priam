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

import com.netflix.priam.backup.*;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.commons.collections4.iterators.FilterIterator;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Do any management task for meta files. Created by aagrawal on 8/2/18. */
public class MetaV2Proxy implements IMetaProxy {
    private static final Logger logger = LoggerFactory.getLogger(MetaV2Proxy.class);
    private final Path metaFileDirectory;
    private final IBackupFileSystem fs;
    private final Provider<AbstractBackupPath> abstractBackupPathProvider;

    @Inject
    public MetaV2Proxy(
            IConfiguration configuration,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> abstractBackupPathProvider) {
        fs = backupFileSystemCtx.getFileStrategy(configuration);
        this.abstractBackupPathProvider = abstractBackupPathProvider;
        metaFileDirectory = Paths.get(configuration.getDataFileLocation());
    }

    @Override
    public Path getLocalMetaFileDirectory() {
        return metaFileDirectory;
    }

    @Override
    public String getMetaPrefix(DateUtil.DateRange dateRange) {
        return getMatch(dateRange, AbstractBackupPath.BackupFileType.META_V2);
    }

    private String getMatch(
            DateUtil.DateRange dateRange, AbstractBackupPath.BackupFileType backupFileType) {
        Path location = fs.getPrefix();
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        String match = StringUtils.EMPTY;
        if (dateRange != null) match = dateRange.match();
        if (dateRange != null && dateRange.getEndTime() == null)
            match = dateRange.getStartTime().toEpochMilli() + "";
        return Paths.get(
                        abstractBackupPath.remoteV2Prefix(location, backupFileType).toString(),
                        match)
                .toString();
    }

    @Override
    public Iterator<AbstractBackupPath> getIncrementals(DateUtil.DateRange dateRange) {
        String incrementalPrefix = getMatch(dateRange, AbstractBackupPath.BackupFileType.SST_V2);
        String marker =
                getMatch(
                        new DateUtil.DateRange(dateRange.getStartTime(), null),
                        AbstractBackupPath.BackupFileType.SST_V2);
        logger.info(
                "Listing filesystem with prefix: {}, marker: {}, daterange: {}",
                incrementalPrefix,
                marker,
                dateRange);
        Iterator<String> iterator = fs.listFileSystem(incrementalPrefix, null, marker);
        Iterator<AbstractBackupPath> transformIterator =
                new TransformIterator<>(
                        iterator,
                        s -> {
                            AbstractBackupPath path = abstractBackupPathProvider.get();
                            path.parseRemote(s);
                            return path;
                        });

        return new FilterIterator<>(
                transformIterator,
                abstractBackupPath ->
                        (abstractBackupPath.getLastModified().isAfter(dateRange.getStartTime())
                                        && abstractBackupPath
                                                .getLastModified()
                                                .isBefore(dateRange.getEndTime()))
                                || abstractBackupPath
                                        .getLastModified()
                                        .equals(dateRange.getStartTime())
                                || abstractBackupPath
                                        .getLastModified()
                                        .equals(dateRange.getEndTime()));
    }

    @Override
    public List<AbstractBackupPath> findMetaFiles(DateUtil.DateRange dateRange) {
        ArrayList<AbstractBackupPath> metas = new ArrayList<>();
        String prefix = getMetaPrefix(dateRange);
        String marker = getMetaPrefix(new DateUtil.DateRange(dateRange.getStartTime(), null));
        logger.info(
                "Listing filesystem with prefix: {}, marker: {}, daterange: {}",
                prefix,
                marker,
                dateRange);
        Iterator<String> iterator = fs.listFileSystem(prefix, null, marker);

        while (iterator.hasNext()) {
            AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
            abstractBackupPath.parseRemote(iterator.next());
            logger.debug("Meta file found: {}", abstractBackupPath);
            if (abstractBackupPath.getLastModified().toEpochMilli()
                            >= dateRange.getStartTime().toEpochMilli()
                    && abstractBackupPath.getLastModified().toEpochMilli()
                            <= dateRange.getEndTime().toEpochMilli()) {
                metas.add(abstractBackupPath);
            }
        }

        metas.sort(Collections.reverseOrder());

        if (metas.size() == 0) {
            logger.info(
                    "No meta file found on remote file system for the time period: {}", dateRange);
        }

        return metas;
    }

    @Override
    public Path downloadMetaFile(AbstractBackupPath meta) throws BackupRestoreException {
        fs.downloadFile(meta, "" /* suffix */, 10 /* retries */);
        return Paths.get(meta.newRestoreFile().getAbsolutePath());
    }

    @Override
    public void cleanupOldMetaFiles() {
        logger.info("Deleting any old META_V2 files if any");
        IOFileFilter fileNameFilter =
                FileFilterUtils.and(
                        FileFilterUtils.prefixFileFilter(MetaFileInfo.META_FILE_PREFIX),
                        FileFilterUtils.or(
                                FileFilterUtils.suffixFileFilter(MetaFileInfo.META_FILE_SUFFIX),
                                FileFilterUtils.suffixFileFilter(
                                        MetaFileInfo.META_FILE_SUFFIX + ".tmp")));
        Collection<File> files =
                FileUtils.listFiles(metaFileDirectory.toFile(), fileNameFilter, null);
        files.stream()
                .filter(File::isFile)
                .forEach(
                        file -> {
                            logger.debug(
                                    "Deleting old META_V2 file found: {}", file.getAbsolutePath());
                            file.delete();
                        });
    }

    @Override
    public List<String> getSSTFilesFromMeta(Path localMetaPath) throws Exception {
        MetaFileBackupWalker metaFileBackupWalker = new MetaFileBackupWalker();
        metaFileBackupWalker.readMeta(localMetaPath);
        return metaFileBackupWalker.backupRemotePaths;
    }

    @Override
    public BackupVerificationResult isMetaFileValid(AbstractBackupPath metaBackupPath) {
        MetaFileBackupValidator metaFileBackupValidator = new MetaFileBackupValidator();
        BackupVerificationResult result = metaFileBackupValidator.verificationResult;
        result.remotePath = metaBackupPath.getRemotePath();
        result.snapshotInstant = metaBackupPath.getLastModified();

        Path metaFile = null;
        try {
            metaFile = downloadMetaFile(metaBackupPath);
            result.manifestAvailable = true;

            metaFileBackupValidator.readMeta(metaFile);
            result.valid = (result.filesInMetaOnly.isEmpty());
        } catch (FileNotFoundException fne) {
            logger.error(fne.getLocalizedMessage());
        } catch (IOException ioe) {
            logger.error(
                    "IO Error while processing meta file: " + metaFile, ioe.getLocalizedMessage());
            ioe.printStackTrace();
        } catch (BackupRestoreException bre) {
            logger.error("Error while trying to download the manifest file: {}", metaBackupPath);
        } finally {
            if (metaFile != null) FileUtils.deleteQuietly(metaFile.toFile());
        }
        return result;
    }

    private class MetaFileBackupValidator extends MetaFileReader {
        private BackupVerificationResult verificationResult = new BackupVerificationResult();

        @Override
        public void process(ColumnFamilyResult columnfamilyResult) {
            for (ColumnFamilyResult.SSTableResult ssTableResult :
                    columnfamilyResult.getSstables()) {
                for (FileUploadResult fileUploadResult : ssTableResult.getSstableComponents()) {
                    if (fs.checkObjectExists(Paths.get(fileUploadResult.getBackupPath()))) {
                        verificationResult.filesMatched++;
                    } else {
                        verificationResult.filesInMetaOnly.add(fileUploadResult.getBackupPath());
                    }
                }
            }
        }
    }

    private class MetaFileBackupWalker extends MetaFileReader {
        private List<String> backupRemotePaths = new ArrayList<>();

        @Override
        public void process(ColumnFamilyResult columnfamilyResult) {
            for (ColumnFamilyResult.SSTableResult ssTableResult :
                    columnfamilyResult.getSstables()) {
                for (FileUploadResult fileUploadResult : ssTableResult.getSstableComponents()) {
                    backupRemotePaths.add(fileUploadResult.getBackupPath());
                }
            }
        }
    }
}
