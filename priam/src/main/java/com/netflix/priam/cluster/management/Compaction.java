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
import com.netflix.priam.merics.CompactionMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.merics.NodeToolFlushMeasurement;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.utils.JMXConnectorMgr;
import org.apache.cassandra.db.Keyspace;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
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

    @Inject
    public Compaction(IConfiguration config, IMetricPublisher metricPublisher) {
        super(config, Task.COMPACTION, metricPublisher, new CompactionMeasurement());
        this.config = config;
    }

    public static final Map<String, List<String>> updateCompactionCFList(IConfiguration config, JMXConnectorMgr jmxConnectorMgr) throws IllegalArgumentException {
        final Map<String, List<String>> columnFamilyFilter = new HashMap<>(); //key: keyspace, value: a list of CFs within the keyspace

        if (config.getCompactionCFList() == null || config.getCompactionCFList().isEmpty())
        {
            logger.info("Compaction: No override provided by user. All keyspaces qualify for compaction.");

            if (jmxConnectorMgr != null)
            jmxConnectorMgr.getKeyspaces().forEach(keyspaceName -> {
                List<String> existingCfs = columnFamilyFilter.getOrDefault(keyspaceName, new ArrayList<>());
                columnFamilyFilter.put(keyspaceName, existingCfs);
            });
            return columnFamilyFilter;
        }

        logger.info("Compaction: Override provided by user. Calculating KS/CF's to qualify for compaction");
        String[] filters = config.getCompactionCFList().split(",");
        for (int i = 0; i < filters.length; i++) { //process each filter
            if (columnFamilyFilterPattern.matcher(filters[i]).find()) {

                String[] filter = filters[i].split("\\.");
                String keyspaceName = filter[0];
                String columnFamilyName = filter[1];

                if (columnFamilyName.indexOf("-") != -1)
                    columnFamilyName = columnFamilyName.substring(0, columnFamilyName.indexOf("-"));

                logger.info("Compaction: Adding CF: {}.{}", keyspaceName, columnFamilyName);

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

    /*
     * @return the keyspace(s) compacted.  List can be empty but never null.
     */
    protected List<String> runTask(JMXConnectorMgr jmxConnectorMgr) throws IllegalArgumentException, TaskException {
        List<String> compactionKeyspaces = new ArrayList<String>();
        final Map<String, List<String>> columnFamilyFilter = Compaction.updateCompactionCFList(this.config, jmxConnectorMgr);

        if (columnFamilyFilter == null || columnFamilyFilter.isEmpty()) {
            logger.warn("No op on requested \"compaction\" as there are no keyspaces.");
            return compactionKeyspaces;
        }

        for(Map.Entry<String, List<String>> entry: columnFamilyFilter.entrySet())
        {
            String keyspace = entry.getKey();
            List<String> columnfamilies = entry.getValue();

            if (!jmxConnectorMgr.getKeyspaces().contains(keyspace))
            {
                logger.error("Keyspace: {} configured for compaction does not exist!", keyspace);
                continue;
            }

            if (SchemaConstant.shouldAvoidKeyspaceForClusterMgmt(keyspace)) //no need to compact system keyspaces.
                continue;

            try{
                if (columnfamilies == null || columnfamilies.isEmpty())
                    jmxConnectorMgr.forceKeyspaceCompaction(keyspace);
                else
                    for(String columnfamily: columnfamilies)
                    {
                        try {
                            jmxConnectorMgr.forceKeyspaceCompaction(keyspace, columnfamily);
                        } catch (IOException | InterruptedException | ExecutionException e) {
                            throw new TaskException("Exception while compacting keyspace: " + keyspace + ", columnfamily: " + columnfamily, e);
                        }
                    };
                compactionKeyspaces.add(keyspace);
            }catch (IOException | InterruptedException | ExecutionException | TaskException e){
                throw new TaskException("Exception while compacting keyspace: " + keyspace, e);
            }
        };

        return compactionKeyspaces;
    }

    private void compact(String keyspace, List<String> columnfamilies, JMXConnectorMgr jmxConnectorMgr) throws TaskException {

    }

    /**
     * Timer to be used for compaction interval.
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
