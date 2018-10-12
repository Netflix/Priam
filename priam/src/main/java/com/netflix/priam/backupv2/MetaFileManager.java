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

import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Do any management task for meta files. Created by aagrawal on 8/2/18. */
public class MetaFileManager {
    private static final Logger logger = LoggerFactory.getLogger(MetaFileManager.class);
    private final Path metaFileDirectory;

    @Inject
    MetaFileManager(IConfiguration configuration) {
        metaFileDirectory = Paths.get(configuration.getDataFileLocation());
    }

    public Path getMetaFileDirectory() {
        return metaFileDirectory;
    }

    /** Delete the old meta files, if any present in the metaFileDirectory */
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
}
