/**
 * Copyright 2018 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.cluster.management;

import com.netflix.priam.backup.BackupRestoreUtil;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.CassandraOperations;
import com.netflix.priam.merics.CompactionMeasurement;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to compact the keyspaces/columnfamilies Created by aagrawal on 1/25/18. */
@Singleton
public class Compaction extends IClusterManagement<String> {
    private static final Logger logger = LoggerFactory.getLogger(Compaction.class);
    private final IConfiguration config;
    private final CassandraOperations cassandraOperations;

    @Inject
    public Compaction(
            IConfiguration config,
            CassandraOperations cassandraOperations,
            CompactionMeasurement compactionMeasurement) {
        super(config, Task.COMPACTION, compactionMeasurement);
        this.config = config;
        this.cassandraOperations = cassandraOperations;
    }

    final Map<String, List<String>> getCompactionIncludeFilter(IConfiguration config)
            throws Exception {
        Map<String, List<String>> columnFamilyFilter =
                BackupRestoreUtil.getFilter(config.getCompactionIncludeCFList());
        logger.info("Compaction: Override for include CF provided by user: {}", columnFamilyFilter);
        return columnFamilyFilter;
    }

    final Map<String, List<String>> getCompactionExcludeFilter(IConfiguration config)
            throws Exception {
        Map<String, List<String>> columnFamilyFilter =
                BackupRestoreUtil.getFilter(config.getCompactionExcludeCFList());
        logger.info("Compaction: Override for exclude CF provided by user: {}", columnFamilyFilter);
        return columnFamilyFilter;
    }

    final Map<String, List<String>> getCompactionFilterCfs(IConfiguration config) throws Exception {
        final Map<String, List<String>> includeFilter = getCompactionIncludeFilter(config);
        final Map<String, List<String>> excludeFilter = getCompactionExcludeFilter(config);
        final Map<String, List<String>> allColumnfamilies = cassandraOperations.getColumnfamilies();
        Map<String, List<String>> result = new HashMap<>();

        allColumnfamilies.forEach(
                (keyspaceName, columnfamilies) -> {
                    if (SchemaConstant.isSystemKeyspace(
                            keyspaceName)) // no need to compact system keyspaces.
                    return;

                    if (excludeFilter != null && excludeFilter.containsKey(keyspaceName)) {
                        List<String> excludeCFFilter = excludeFilter.get(keyspaceName);
                        // Is CF list null/empty? If yes, then exclude all CF's for this keyspace.
                        if (excludeCFFilter == null || excludeCFFilter.isEmpty()) return;

                        columnfamilies =
                                (List<String>)
                                        CollectionUtils.removeAll(columnfamilies, excludeCFFilter);
                    }

                    if (includeFilter != null) {
                        // Include filter is not empty and this keyspace is not provided in include
                        // filter. Ignore processing of this keyspace.
                        if (!includeFilter.containsKey(keyspaceName)) return;

                        List<String> includeCFFilter = includeFilter.get(keyspaceName);
                        // If include filter is empty or null, it means include all.
                        // If not, then we need to find intersection of CF's which are present and
                        // one which are configured to compact.
                        if (includeCFFilter != null
                                && !includeCFFilter
                                        .isEmpty()) // If include filter is empty or null, it means
                            // include all.
                            columnfamilies =
                                    (List<String>)
                                            CollectionUtils.intersection(
                                                    columnfamilies, includeCFFilter);
                    }

                    if (columnfamilies != null && !columnfamilies.isEmpty())
                        result.put(keyspaceName, columnfamilies);
                });

        return result;
    }

    /*
     * @return the keyspace(s) compacted.  List can be empty but never null.
     */
    protected String runTask() throws Exception {
        final Map<String, List<String>> columnfamilies = getCompactionFilterCfs(config);

        if (!columnfamilies.isEmpty())
            for (Map.Entry<String, List<String>> entry : columnfamilies.entrySet()) {
                cassandraOperations.forceKeyspaceCompaction(
                        entry.getKey(), entry.getValue().toArray(new String[0]));
            }

        return columnfamilies.toString();
    }

    /**
     * Timer to be used for compaction interval.
     *
     * @param config {@link IConfiguration} to get configuration details from priam.
     * @return the timer to be used for compaction interval from {@link
     *     IConfiguration#getCompactionCronExpression()}
     * @throws Exception If the cron expression is invalid.
     */
    public static TaskTimer getTimer(IConfiguration config) throws Exception {
        return CronTimer.getCronTimer(Task.COMPACTION.name(), config.getCompactionCronExpression());
    }
}
