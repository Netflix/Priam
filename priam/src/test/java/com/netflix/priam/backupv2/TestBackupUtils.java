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

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;

/** Created by aagrawal on 12/17/18. */
public class TestBackupUtils {
    private MetaFileWriterBuilder metaFileWriterBuilder;
    private Provider<AbstractBackupPath> pathProvider;
    protected final String keyspace = "keyspace1";
    protected final String columnfamily = "columnfamily1";
    private final String dataDir;

    @Inject
    public TestBackupUtils() {
        Injector injector = Guice.createInjector(new BRTestModule());
        pathProvider = injector.getProvider(AbstractBackupPath.class);
        metaFileWriterBuilder = injector.getInstance(MetaFileWriterBuilder.class);
        dataDir = injector.getInstance(IConfiguration.class).getDataFileLocation();
    }

    public Path createMeta(List<String> filesToAdd, Instant snapshotTime) throws IOException {
        MetaFileWriterBuilder.DataStep dataStep =
                metaFileWriterBuilder.newBuilder().startMetaFileGeneration(snapshotTime);
        ColumnfamilyResult columnfamilyResult = new ColumnfamilyResult(keyspace, columnfamily);
        for (String file : filesToAdd) {
            AbstractBackupPath path = pathProvider.get();
            path.parseRemote(file);
            if (path.getType() == AbstractBackupPath.BackupFileType.SST_V2) {
                ColumnfamilyResult.SSTableResult ssTableResult =
                        new ColumnfamilyResult.SSTableResult();

                ssTableResult.setSstableComponents(new ArrayList<>());
                ssTableResult.getSstableComponents().add(getFileUploadResult(path));
                columnfamilyResult.addSstable(ssTableResult);
            }
        }
        dataStep.addColumnfamilyResult(columnfamilyResult);
        Path metaPath = dataStep.endMetaFileGeneration().getMetaFilePath();
        metaPath.toFile().setLastModified(snapshotTime.toEpochMilli());
        return metaPath;
    }

    public String createFile(String fileName, Instant lastModifiedTime) throws Exception {
        Path path = Paths.get(dataDir, keyspace, columnfamily, fileName);
        FileUtils.forceMkdirParent(path.toFile());
        try (FileWriter fileWriter = new FileWriter(path.toFile())) {
            fileWriter.write("");
        }
        path.toFile().setLastModified(lastModifiedTime.toEpochMilli());
        return path.toString();
    }

    private FileUploadResult getFileUploadResult(AbstractBackupPath path) {
        FileUploadResult fileUploadResult =
                new FileUploadResult(
                        Paths.get(path.getFileName()),
                        path.getKeyspace(),
                        path.getColumnFamily(),
                        path.getLastModified(),
                        path.getLastModified(),
                        path.getSize());
        fileUploadResult.setBackupPath(path.getRemotePath());
        return fileUploadResult;
    }
}
