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

package com.netflix.priam.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import org.apache.cassandra.io.sstable.Component;
import org.apache.commons.io.FileUtils;

/** Created by aagrawal on 9/23/18. */
public class BackupFileUtils {
    public static void cleanupDir(Path dir) {
        if (dir.toFile().exists())
            try {
                FileUtils.cleanDirectory(dir.toFile());
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public static void generateDummyFiles(
            Path dummyDir,
            int noOfKeyspaces,
            int noOfCf,
            int noOfSstables,
            String backupDir,
            String snapshotName,
            boolean cleanup)
            throws Exception {
        // Clean the dummy directory
        if (cleanup) cleanupDir(dummyDir);

        for (int i = 1; i <= noOfKeyspaces; i++) {
            String keyspaceName = "sample" + i;

            for (int j = 1; j <= noOfCf; j++) {
                String columnfamilyname = "cf" + j;

                for (int k = 1; k <= noOfSstables; k++) {
                    String prefixName = "mc-" + k + "-big";

                    for (Component.Type type : EnumSet.allOf(Component.Type.class)) {
                        Path componentPath =
                                Paths.get(
                                        dummyDir.toFile().getAbsolutePath(),
                                        keyspaceName,
                                        columnfamilyname,
                                        backupDir,
                                        snapshotName,
                                        prefixName + "-" + type.name() + ".db");
                        componentPath.getParent().toFile().mkdirs();
                        try (FileWriter fileWriter = new FileWriter(componentPath.toFile())) {
                            fileWriter.write("");
                        }
                    }
                }

                Path componentPath =
                        Paths.get(
                                dummyDir.toFile().getAbsolutePath(),
                                keyspaceName,
                                columnfamilyname,
                                backupDir,
                                snapshotName,
                                "manifest.json");
                try (FileWriter fileWriter = new FileWriter(componentPath.toFile())) {
                    fileWriter.write("");
                }
            }
        }
    }
}
