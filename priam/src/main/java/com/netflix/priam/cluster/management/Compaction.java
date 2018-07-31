/**
 * Copyright 2018 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.cluster.management;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraOperations;
import com.netflix.priam.merics.CompactionMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Utility class to compact the keyspaces/columnfamilies
 * Created by aagrawal on 1/25/18.
 */
@Singleton
public class Compaction extends IClusterManagement<String> {
    private static final Logger logger = LoggerFactory.getLogger(Compaction.class);
    private final IConfiguration config;
    private static final Pattern columnFamilyFilterPattern = Pattern.compile(".\\..");
    private final CassandraOperations cassandraOperations;

    @Inject
    public Compaction(IConfiguration config, IMetricPublisher metricPublisher, CassandraOperations cassandraOperations) {
        super(config, Task.COMPACTION, metricPublisher, new CompactionMeasurement());
        this.config = config;
        this.cassandraOperations = cassandraOperations;
    }

    private final Map<String, List<String>> getCompactionFilter(String compactionFilter) throws IllegalArgumentException {
        if (StringUtils.isEmpty(compactionFilter))
            return null;

        final Map<String, List<String>> columnFamilyFilter = new HashMap<>(); //key: keyspace, value: a list of CFs within the keyspace

        String[] filters = compactionFilter.split(",");
        for (int i = 0; i < filters.length; i++) { //process each filter
            if (columnFamilyFilterPattern.matcher(filters[i]).find()) {

                String[] filter = filters[i].split("\\.");
                String keyspaceName = filter[0];
                String columnFamilyName = filter[1];

                if (columnFamilyName.indexOf("-") != -1)
                    columnFamilyName = columnFamilyName.substring(0, columnFamilyName.indexOf("-"));

                List<String> existingCfs = columnFamilyFilter.getOrDefault(keyspaceName, new ArrayList<>());
                if (!columnFamilyName.equalsIgnoreCase("*"))
                    existingCfs.add(columnFamilyName);
                columnFamilyFilter.put(keyspaceName, existingCfs);

            } else {
                throw new IllegalArgumentException("Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: " + filters[i]);
            }
        }
        return columnFamilyFilter;
    }

    final Map<String, List<String>> getCompactionIncludeFilter(IConfiguration config) throws Exception {
        if (StringUtils.isEmpty(config.getCompactionIncludeCFList()))
            return null;

        Map<String, List<String>> columnFamilyFilter = getCompactionFilter(config.getCompactionIncludeCFList());
        logger.info("Compaction: Override for include CF provided by user: {}", columnFamilyFilter);
        return columnFamilyFilter;
    }

    final Map<String, List<String>> getCompactionExcludeFilter(IConfiguration config) throws Exception {
        if (StringUtils.isEmpty(config.getCompactionExcludeCFList()))
            return null;

        Map<String, List<String>> columnFamilyFilter = getCompactionFilter(config.getCompactionExcludeCFList());
        logger.info("Compaction: Override for exclude CF provided by user: {}", columnFamilyFilter);
        return columnFamilyFilter;
    }

    final Map<String, List<String>> getCompactionFilterCfs(IConfiguration config) throws Exception {
        final Map<String, List<String>> includeFilter = getCompactionIncludeFilter(config);
        final Map<String, List<String>> excludeFilter = getCompactionExcludeFilter(config);
        final Map<String, List<String>> allColumnfamilies = cassandraOperations.getColumnfamilies();
        Map<String, List<String>> result = new HashMap<>();


        allColumnfamilies.entrySet().forEach(entry -> {
            String keyspaceName = entry.getKey();
            if (SchemaConstant.isSystemKeyspace(keyspaceName)) //no need to compact system keyspaces.
                return;

            List<String> columnfamilies = entry.getValue();
            if (excludeFilter != null && excludeFilter.containsKey(keyspaceName)) {
                List<String> excludeCFFilter = excludeFilter.get(keyspaceName);
                //Is CF list null/empty? If yes, then exclude all CF's for this keyspace.
                if (excludeCFFilter == null || excludeCFFilter.isEmpty())
                    return;

                columnfamilies = (List<String>) CollectionUtils.removeAll(columnfamilies, excludeCFFilter);
            }

            if (includeFilter != null) {
                //Include filter is not empty and this keyspace is not provided in include filter. Ignore processing of this keyspace.
                if (!includeFilter.containsKey(keyspaceName))
                    return;

                List<String> includeCFFilter = includeFilter.get(keyspaceName);
                //If include filter is empty or null, it means include all.
                //If not, then we need to find intersection of CF's which are present and one which are configured to compact.
                if (includeCFFilter != null && !includeCFFilter.isEmpty()) //If include filter is empty or null, it means include all.
                    columnfamilies = (List<String>) CollectionUtils.intersection(columnfamilies, includeCFFilter);
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
                cassandraOperations.forceKeyspaceCompaction(entry.getKey(), entry.getValue().toArray(new String[0]));
            }

        return columnfamilies.toString();
    }

    /**
     * Timer to be used for compaction interval.
     *
     * @param config {@link IConfiguration} to get configuration details from priam.
     * @return the timer to be used for compaction interval  from {@link IConfiguration#getCompactionCronExpression()}
     */
    public static TaskTimer getTimer(IConfiguration config) throws Exception {

        CronTimer cronTimer = null;

        String cronExpression = config.getCompactionCronExpression();

        if (StringUtils.isEmpty(cronExpression)) {
            logger.info("Skipping compaction as compaction cron is not set.");
        } else {
            if (!CronExpression.isValidExpression(cronExpression))
                throw new Exception("Invalid CRON expression: " + cronExpression +
                        ". Please remove cron expression if you wish to disable compaction else fix the CRON expression and try again!");

            cronTimer = new CronTimer(Task.COMPACTION.name(), cronExpression);
            logger.info("Starting compaction with CRON expression {}", cronTimer.getCronExpression());
        }

        return cronTimer;
    }

}
