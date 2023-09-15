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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.stream.JsonWriter;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will help in generation of meta.json files. This will encapsulate all the SSTables
 * that were there on the file system. This will write the meta.json file as a JSON blob. NOTE: We
 * want to ensure that it is done via streaming JSON write to ensure we do not consume memory to
 * load all these objects in memory. With multi-tenant clusters or LCS enabled on large number of
 * CF's it is easy to have 1000's of SSTables (thus 1000's of SSTable components) across CF's.
 * Created by aagrawal on 6/12/18.
 */
public class MetaFileWriterBuilder {
    private final MetaFileWriter metaFileWriter;
    private static final Logger logger = LoggerFactory.getLogger(MetaFileWriterBuilder.class);

    @Inject
    public MetaFileWriterBuilder(MetaFileWriter metaFileWriter) {
        this.metaFileWriter = metaFileWriter;
    }

    public StartStep newBuilder() throws IOException {
        return metaFileWriter;
    }

    public interface StartStep {
        DataStep startMetaFileGeneration(Instant snapshotInstant) throws IOException;
    }

    public interface DataStep {
        ColumnFamilyResult addColumnfamilyResult(
                String keyspace,
                String columnFamily,
                ImmutableMultimap<String, AbstractBackupPath> sstables)
                throws IOException;

        UploadStep endMetaFileGeneration() throws IOException;
    }

    public interface UploadStep {
        void uploadMetaFile() throws Exception;

        Path getMetaFilePath();

        String getRemoteMetaFilePath() throws Exception;
    }

    public static class MetaFileWriter implements StartStep, DataStep, UploadStep {
        private final Provider<AbstractBackupPath> pathFactory;
        private final IBackupFileSystem backupFileSystem;

        private final MetaFileInfo metaFileInfo;
        private final IMetaProxy metaProxy;
        private JsonWriter jsonWriter;
        private Instant snapshotInstant;
        private Path metaFilePath;

        @Inject
        public MetaFileWriter(
                IConfiguration configuration,
                InstanceIdentity instanceIdentity,
                Provider<AbstractBackupPath> pathFactory,
                IFileSystemContext backupFileSystemCtx,
                @Named("v2") IMetaProxy metaProxy) {
            this.pathFactory = pathFactory;
            this.backupFileSystem = backupFileSystemCtx.getFileStrategy(configuration);
            this.metaProxy = metaProxy;
            List<String> backupIdentifier = new ArrayList<>();
            backupIdentifier.add(instanceIdentity.getInstance().getToken());
            metaFileInfo =
                    new MetaFileInfo(
                            configuration.getAppName(),
                            instanceIdentity.getInstanceInfo().getRegion(),
                            instanceIdentity.getInstanceInfo().getRac(),
                            backupIdentifier);
        }

        /**
         * Start the generation of meta file.
         *
         * @throws IOException if unable to write to meta file (permissions, disk full etc)
         */
        public DataStep startMetaFileGeneration(Instant snapshotInstant) throws IOException {
            // Compute meta file name.
            this.snapshotInstant = snapshotInstant;
            String fileName = MetaFileInfo.getMetaFileName(snapshotInstant);
            metaFilePath = Paths.get(metaProxy.getLocalMetaFileDirectory().toString(), fileName);
            Path tempMetaFilePath =
                    Paths.get(metaProxy.getLocalMetaFileDirectory().toString(), fileName + ".tmp");

            logger.info("Starting to write a new meta file: {}", metaFilePath);

            jsonWriter = new JsonWriter(new FileWriter(tempMetaFilePath.toFile()));
            jsonWriter.beginObject();
            jsonWriter.name(MetaFileInfo.META_FILE_INFO);
            jsonWriter.jsonValue(metaFileInfo.toString());
            jsonWriter.name(MetaFileInfo.META_FILE_DATA);
            jsonWriter.beginArray();
            return this;
        }

        /**
         * Add {@link ColumnFamilyResult} after it has been processed so it can be streamed to
         * meta.json. Streaming write to meta.json is required so we don't get Priam OOM.
         *
         * @throws IOException if unable to write to the file or if JSON is not valid
         */
        public ColumnFamilyResult addColumnfamilyResult(
                String keyspace,
                String columnFamily,
                ImmutableMultimap<String, AbstractBackupPath> sstables)
                throws IOException {

            if (jsonWriter == null)
                throw new NullPointerException(
                        "addColumnfamilyResult: Json Writer in MetaFileWriter is null. This should not happen!");
            ColumnFamilyResult result = toColumnFamilyResult(keyspace, columnFamily, sstables);
            jsonWriter.jsonValue(result.toString());
            return result;
        }

        /**
         * Finish the generation of meta.json file and save it on local media.
         *
         * @return {@link Path} to the local meta.json produced.
         * @throws IOException if unable to write to file or if JSON is not valid
         */
        public MetaFileWriterBuilder.UploadStep endMetaFileGeneration() throws IOException {
            if (jsonWriter == null)
                throw new NullPointerException(
                        "endMetaFileGeneration: Json Writer in MetaFileWriter is null. This should not happen!");

            jsonWriter.endArray();
            jsonWriter.endObject();
            jsonWriter.close();

            Path tempMetaFilePath =
                    Paths.get(
                            metaProxy.getLocalMetaFileDirectory().toString(),
                            metaFilePath.toFile().getName() + ".tmp");

            // Rename the tmp file.
            tempMetaFilePath.toFile().renameTo(metaFilePath.toFile());

            // Set the last modified time to snapshot time as generating manifest file may take some
            // time.
            metaFilePath.toFile().setLastModified(snapshotInstant.toEpochMilli());

            logger.info("Finished writing to meta file: {}", metaFilePath);

            return this;
        }

        /**
         * Upload the meta file generated to backup file system.
         *
         * @throws Exception when unable to upload the meta file.
         */
        public void uploadMetaFile() throws Exception {
            AbstractBackupPath abstractBackupPath = pathFactory.get();
            abstractBackupPath.parseLocal(
                    metaFilePath.toFile(), AbstractBackupPath.BackupFileType.META_V2);
            backupFileSystem.uploadAndDelete(abstractBackupPath, false /* async */);
        }

        public Path getMetaFilePath() {
            return metaFilePath;
        }

        public String getRemoteMetaFilePath() throws Exception {
            AbstractBackupPath abstractBackupPath = pathFactory.get();
            abstractBackupPath.parseLocal(
                    metaFilePath.toFile(), AbstractBackupPath.BackupFileType.META_V2);
            return abstractBackupPath.getRemotePath();
        }

        private ColumnFamilyResult toColumnFamilyResult(
                String keyspace,
                String columnFamily,
                ImmutableMultimap<String, AbstractBackupPath> sstables) {
            ColumnFamilyResult columnfamilyResult = new ColumnFamilyResult(keyspace, columnFamily);
            sstables.keySet()
                    .stream()
                    .map(k -> toSSTableResult(k, sstables.get(k)))
                    .forEach(columnfamilyResult::addSstable);
            return columnfamilyResult;
        }

        private ColumnFamilyResult.SSTableResult toSSTableResult(
                String prefix, ImmutableCollection<AbstractBackupPath> sstable) {
            ColumnFamilyResult.SSTableResult ssTableResult = new ColumnFamilyResult.SSTableResult();
            ssTableResult.setPrefix(prefix);
            ssTableResult.setSstableComponents(
                    ImmutableSet.copyOf(
                            sstable.stream()
                                    .map(this::toFileUploadResult)
                                    .collect(Collectors.toSet())));
            return ssTableResult;
        }

        private FileUploadResult toFileUploadResult(AbstractBackupPath path) {
            FileUploadResult fileUploadResult = new FileUploadResult(path);
            try {
                Path backupPath = Paths.get(fileUploadResult.getBackupPath());
                fileUploadResult.setUploaded(backupFileSystem.checkObjectExists(backupPath));
            } catch (Exception e) {
                logger.error("Error checking if file exists. Ignoring as it is not fatal.", e);
            }
            return fileUploadResult;
        }
    }
}
