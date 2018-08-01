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

import com.google.gson.stream.JsonWriter;
import com.google.inject.Provider;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.RetryableCallable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class will help in generation of meta.json files. This will encapsulate all the SSTables that were there
 * on the file system. This will write the meta.json file as a JSON blob.
 * NOTE:  We want to ensure that it is done via streaming JSON write to ensure we do not consume memory to load all
 * these objects in memory. With multi-tenant clusters or LCS enabled on large number of CF's it is easy to have 1000's
 * of SSTables (thus 1000's of SSTable components) across CF's.
 * Created by aagrawal on 6/12/18.
 */
public class MetaFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(MetaFileWriter.class);
    private final Provider<AbstractBackupPath> pathFactory;
    private final IBackupFileSystem backupFileSystem;

    private MetaFileInfo metaFileInfo;
    private JsonWriter jsonWriter;
    private Path metaFilePath;
    private final Path metaFileDirectory;

    @Inject
    MetaFileWriter(IConfiguration configuration, InstanceIdentity instanceIdentity, Provider<AbstractBackupPath> pathFactory, IFileSystemContext backupFileSystemCtx) {
        this.pathFactory = pathFactory;
        this.backupFileSystem = backupFileSystemCtx.getFileStrategy(configuration);
        List<String> backupIdentifier = new ArrayList<>();
        backupIdentifier.add(instanceIdentity.getInstance().getToken());
        metaFileInfo = new MetaFileInfo(configuration.getAppName(), configuration.getDC(), configuration.getRac(), backupIdentifier);
        metaFileDirectory = Paths.get(configuration.getDataFileLocation());
    }

    /**
     * Start the generation of meta file.
     *
     * @throws IOException
     */
    public void startMetaFileGeneration() throws IOException {
        //Compute meta file name.
        String fileName = MetaFileInfo.getMetaFileName();
        metaFilePath = Paths.get(metaFileDirectory.toString(), fileName);
        Path tempMetaFilePath = Paths.get(metaFileDirectory.toString(), fileName + ".tmp");

        logger.info("Starting to write a new meta file: {}", metaFilePath);

        jsonWriter = new JsonWriter(new FileWriter(tempMetaFilePath.toFile()));
        jsonWriter.beginObject();
        jsonWriter.name(MetaFileInfo.META_FILE_INFO);
        jsonWriter.jsonValue(metaFileInfo.toString());
        jsonWriter.name(MetaFileInfo.META_FILE_DATA);
        jsonWriter.beginArray();
    }

    /**
     * Add {@link ColumnfamilyResult} after it has been processed so it can be streamed to meta.json. Streaming write to meta.json is required so we don't get Priam OOM.
     *
     * @param columnfamilyResult a POJO encapsulating the column family result
     * @throws IOException
     */
    public void addColumnfamilyResult(ColumnfamilyResult columnfamilyResult) throws IOException {
        if (jsonWriter != null && columnfamilyResult != null)
            jsonWriter.jsonValue(columnfamilyResult.toString());
    }

    /**
     * Finish the generation of meta.json file and save it on local media.
     *
     * @return {@link Path} to the local meta.json produced.
     * @throws IOException
     */
    public Path endMetaFileGeneration() throws IOException {
        if (jsonWriter == null)
            return null;

        jsonWriter.endArray();
        jsonWriter.endObject();
        jsonWriter.close();

        Path tempMetaFilePath = Paths.get(metaFileDirectory.toString(), metaFilePath.toFile().getName() + ".tmp");

        //Rename the tmp file.
        tempMetaFilePath.toFile().renameTo(metaFilePath.toFile());
        logger.info("Finished writing to meta file: {}", metaFilePath);

        return metaFilePath;
    }

    /**
     * Upload the meta file generated to backup file system.
     *
     * @param metafile        {@link Path} to the local meta file that needs to be backed up.
     * @param deleteOnSuccess delete the meta file from local file system if backup is successful. Useful for testing purposes
     * @throws Exception when unable to upload the meta file.
     */
    public void uploadMetaFile(Path metafile, boolean deleteOnSuccess) throws Exception {
        AbstractBackupPath abstractBackupPath = pathFactory.get();
        abstractBackupPath.parseLocal(metafile.toFile(), AbstractBackupPath.BackupFileType.META_V2);
        new RetryableCallable<Void>() {
            @Override
            public Void retriableCall() throws Exception {
                backupFileSystem.upload(abstractBackupPath, abstractBackupPath.localReader());
                return null;
            }
        }.call();
        abstractBackupPath.setCompressedFileSize(backupFileSystem.getBytesUploaded());

        if (deleteOnSuccess)
            FileUtils.deleteQuietly(metafile.toFile());
    }

    /**
     * Delete the old meta files, if any present in the metaFileDirectory
     */
    public void cleanupOldMetaFiles(){
        logger.info("Deleting any old META_V2 files if any");
        IOFileFilter fileNameFilter = FileFilterUtils.and(FileFilterUtils.prefixFileFilter(MetaFileInfo.META_FILE_PREFIX),
                FileFilterUtils.or(FileFilterUtils.suffixFileFilter(MetaFileInfo.META_FILE_SUFFIX),
                        FileFilterUtils.suffixFileFilter(MetaFileInfo.META_FILE_SUFFIX + ".tmp")));
        Collection<File> files = FileUtils.listFiles(metaFileDirectory.toFile(), fileNameFilter, null);
        files.stream().filter(file -> file.isFile()).forEach(file -> {
            logger.debug("Deleting old META_V2 file found: {}", file.getAbsolutePath());
            file.delete();
        });
    }
}
