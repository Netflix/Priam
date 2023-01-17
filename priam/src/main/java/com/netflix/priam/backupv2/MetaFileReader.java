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

import com.google.gson.stream.JsonReader;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class encapsulates the reading of meta file in streaming fashion. This is required
 * as we could have a meta file which cannot fit in memory. Created by aagrawal on 7/3/18.
 */
public abstract class MetaFileReader {

    private static final Logger logger = LoggerFactory.getLogger(MetaFileReader.class);
    private MetaFileInfo metaFileInfo;

    public MetaFileInfo getMetaFileInfo() {
        return metaFileInfo;
    }

    /**
     * Reads the local meta file as denoted by metaFilePath.
     *
     * @param metaFilePath local file path for the meta file.
     * @throws IOException if not enough permissions or file is not valid format.
     */
    public void readMeta(Path metaFilePath) throws IOException {
        // Validate if meta file exists and is right file name.
        if (metaFilePath == null
                || !metaFilePath.toFile().exists()
                || !metaFilePath.toFile().isFile()
                || !isValidMetaFile(metaFilePath)) {
            throw new FileNotFoundException(
                    "MetaFilePath: " + metaFilePath + " do not exist or is not valid meta file.");
        }

        // Read the meta file.
        logger.info("Trying to read the meta file: {}", metaFilePath);
        JsonReader jsonReader = new JsonReader(new FileReader(metaFilePath.toFile()));
        jsonReader.beginObject();
        while (jsonReader.hasNext()) {
            switch (jsonReader.nextName()) {
                case MetaFileInfo.META_FILE_INFO:
                    metaFileInfo =
                            GsonJsonSerializer.getGson().fromJson(jsonReader, MetaFileInfo.class);
                    break;
                case MetaFileInfo.META_FILE_DATA:
                    jsonReader.beginArray();
                    while (jsonReader.hasNext())
                        process(
                                GsonJsonSerializer.getGson()
                                        .fromJson(jsonReader, ColumnFamilyResult.class));
                    jsonReader.endArray();
            }
        }
        jsonReader.endObject();
        jsonReader.close();
        logger.info("Finished reading the meta file: {}", metaFilePath);
    }

    /**
     * Process the columnfamily result obtained after reading meta file.
     *
     * @param columnfamilyResult {@link ColumnFamilyResult} POJO containing the column family data
     *     (all SSTables references) obtained from meta.json.
     */
    public abstract void process(ColumnFamilyResult columnfamilyResult);

    /**
     * Returns if it is a valid meta file name.
     *
     * @param metaFilePath Path to the local meta file
     * @return true, if metafile name is valid.
     */
    public boolean isValidMetaFile(Path metaFilePath) {
        String fileName = metaFilePath.toFile().getName();
        if (fileName.startsWith(MetaFileInfo.META_FILE_PREFIX)
                && fileName.endsWith(MetaFileInfo.META_FILE_SUFFIX)) {
            // is valid date?
            String dateString =
                    fileName.substring(
                            MetaFileInfo.META_FILE_PREFIX.length(),
                            fileName.length() - MetaFileInfo.META_FILE_SUFFIX.length());
            DateUtil.parseInstant(dateString);
            return true;
        }

        return false;
    }
}
