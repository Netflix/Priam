/*
 * Copyright 2017 Netflix, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.backupv2.MetaV2Proxy;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 8/14/17. */
public class BackupRestoreUtil {
    private static final Logger logger = LoggerFactory.getLogger(BackupRestoreUtil.class);
    private static final Pattern columnFamilyFilterPattern = Pattern.compile(".\\..");
    private Map<String, List<String>> includeFilter;
    private Map<String, List<String>> excludeFilter;

    public static final List<String> FILTER_KEYSPACE = Collections.singletonList("OpsCenter");
    private static final Map<String, List<String>> FILTER_COLUMN_FAMILY =
            ImmutableMap.of(
                    "system",
                    Arrays.asList(
                            "local", "peers", "hints", "compactions_in_progress", "LocationInfo"));

    @Inject
    public BackupRestoreUtil(String configIncludeFilter, String configExcludeFilter) {
        setFilters(configIncludeFilter, configExcludeFilter);
    }

    public BackupRestoreUtil setFilters(String configIncludeFilter, String configExcludeFilter) {
        includeFilter = getFilter(configIncludeFilter);
        excludeFilter = getFilter(configExcludeFilter);
        logger.info("Exclude filter set: {}", configExcludeFilter);
        logger.info("Include filter set: {}", configIncludeFilter);
        return this;
    }

    public static Optional<AbstractBackupPath> getLatestValidMetaPath(
            IMetaProxy metaProxy, DateUtil.DateRange dateRange) {
        // Get a list of manifest files.
        List<AbstractBackupPath> metas = metaProxy.findMetaFiles(dateRange);

        // Find a valid manifest file.
        for (AbstractBackupPath meta : metas) {
            BackupVerificationResult result = metaProxy.isMetaFileValid(meta);
            if (result.valid) {
                return Optional.of(meta);
            }
        }

        return Optional.empty();
    }

    public static List<AbstractBackupPath> getAllFiles(
            AbstractBackupPath latestValidMetaFile,
            DateUtil.DateRange dateRange,
            IMetaProxy metaProxy,
            Provider<AbstractBackupPath> pathProvider)
            throws Exception {
        // Download the meta.json file.
        Path metaFile = metaProxy.downloadMetaFile(latestValidMetaFile);
        // Parse meta.json file to find the files required to download from this snapshot.
        List<AbstractBackupPath> allFiles =
                metaProxy
                        .getSSTFilesFromMeta(metaFile)
                        .stream()
                        .map(
                                value -> {
                                    AbstractBackupPath path = pathProvider.get();
                                    path.parseRemote(value);
                                    return path;
                                })
                        .collect(Collectors.toList());

        FileUtils.deleteQuietly(metaFile.toFile());

        // Download incremental SSTables after the snapshot meta file.
        Instant snapshotTime;
        if (metaProxy instanceof MetaV2Proxy) snapshotTime = latestValidMetaFile.getLastModified();
        else snapshotTime = latestValidMetaFile.getTime().toInstant();

        DateUtil.DateRange incrementalDateRange =
                new DateUtil.DateRange(snapshotTime, dateRange.getEndTime());
        Iterator<AbstractBackupPath> incremental = metaProxy.getIncrementals(incrementalDateRange);
        while (incremental.hasNext()) allFiles.add(incremental.next());

        return allFiles;
    }

    public static final Map<String, List<String>> getFilter(String inputFilter)
            throws IllegalArgumentException {
        if (StringUtils.isEmpty(inputFilter)) return null;

        final Map<String, List<String>> columnFamilyFilter =
                new HashMap<>(); // key: keyspace, value: a list of CFs within the keyspace

        String[] filters = inputFilter.split(",");
        for (String cfFilter :
                filters) { // process filter of form keyspace.* or keyspace.columnfamily
            if (columnFamilyFilterPattern.matcher(cfFilter).find()) {

                String[] filter = cfFilter.split("\\.");
                String keyspaceName = filter[0];
                String columnFamilyName = filter[1];

                if (columnFamilyName.contains("-"))
                    columnFamilyName = columnFamilyName.substring(0, columnFamilyName.indexOf("-"));

                List<String> existingCfs =
                        columnFamilyFilter.getOrDefault(keyspaceName, new ArrayList<>());
                if (!columnFamilyName.equalsIgnoreCase("*")) existingCfs.add(columnFamilyName);
                columnFamilyFilter.put(keyspaceName, existingCfs);

            } else {
                throw new IllegalArgumentException(
                        "Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: "
                                + cfFilter);
            }
        }
        return columnFamilyFilter;
    }

    /**
     * Returns if provided keyspace and/or columnfamily is filtered for backup or restore.
     *
     * @param keyspace name of the keyspace in consideration
     * @param columnFamilyDir name of the columnfamily directory in consideration
     * @return true if directory should be filter from processing; otherwise, false.
     */
    public final boolean isFiltered(String keyspace, String columnFamilyDir) {
        if (StringUtils.isEmpty(keyspace) || StringUtils.isEmpty(columnFamilyDir)) return false;

        String columnFamilyName = columnFamilyDir.split("-")[0];
        // column family is in list of global CF filter
        if (FILTER_COLUMN_FAMILY.containsKey(keyspace)
                && FILTER_COLUMN_FAMILY.get(keyspace).contains(columnFamilyName)) return true;

        if (excludeFilter != null)
            if (excludeFilter.containsKey(keyspace)
                    && (excludeFilter.get(keyspace).isEmpty()
                            || excludeFilter.get(keyspace).contains(columnFamilyName))) {
                logger.debug(
                        "Skipping: keyspace: {}, CF: {} is part of exclude list.",
                        keyspace,
                        columnFamilyName);
                return true;
            }

        if (includeFilter != null)
            if (!(includeFilter.containsKey(keyspace)
                    && (includeFilter.get(keyspace).isEmpty()
                            || includeFilter.get(keyspace).contains(columnFamilyName)))) {
                logger.debug(
                        "Skipping: keyspace: {}, CF: {} is not part of include list.",
                        keyspace,
                        columnFamilyName);
                return true;
            }

        return false;
    }
}
