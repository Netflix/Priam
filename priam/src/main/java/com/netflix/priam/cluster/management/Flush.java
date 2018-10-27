/**
 * Copyright 2017 Netflix, Inc.
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

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraOperations;
import com.netflix.priam.merics.NodeToolFlushMeasurement;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility to flush Keyspaces from memtable to disk Created by vinhn on 10/12/16. */
@Singleton
public class Flush extends IClusterManagement<String> {
    private static final Logger logger = LoggerFactory.getLogger(Flush.class);

    private final IConfiguration config;
    private final CassandraOperations cassandraOperations;
    private List<String> keyspaces = new ArrayList<>();

    @Inject
    public Flush(
            IConfiguration config,
            CassandraOperations cassandraOperations,
            NodeToolFlushMeasurement nodeToolFlushMeasurement) {
        super(config, Task.FLUSH, nodeToolFlushMeasurement);
        this.config = config;
        this.cassandraOperations = cassandraOperations;
    }

    @Override
    /*
     * @return the keyspace(s) flushed.  List can be empty but never null.
     */
    protected String runTask() throws Exception {
        List<String> flushed = new ArrayList<>();

        // Get keyspaces to flush
        deriveKeyspaces();

        if (this.keyspaces == null || this.keyspaces.isEmpty()) {
            logger.warn("NO op on requested \"flush\" as there are no keyspaces.");
            return flushed.toString();
        }

        // If flush is for certain keyspaces, validate keyspace exist
        for (String keyspace : keyspaces) {
            if (!cassandraOperations.getKeyspaces().contains(keyspace)) {
                throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
            }

            if (SchemaConstant.isSystemKeyspace(keyspace)) // no need to flush system keyspaces.
            continue;

            try {
                cassandraOperations.forceKeyspaceFlush(keyspace);
                flushed.add(keyspace);
            } catch (IOException | ExecutionException | InterruptedException e) {
                throw new Exception("Exception during flushing keyspace: " + keyspace, e);
            }
        }

        return flushed.toString();
    }

    /*
    Derive keyspace(s) to flush in the following order:  explicit list provided by caller, property, or all keyspaces.
     */
    private void deriveKeyspaces() throws Exception {
        // == get value from property
        String raw = this.config.getFlushKeyspaces();
        if (!StringUtils.isEmpty(raw)) {
            String k[] = raw.split(",");
            for (int i = 0; i < k.length; i++) {
                this.keyspaces.add(i, k[i]);
            }

            return;
        }

        // == no override via FP, default to all keyspaces
        this.keyspaces = cassandraOperations.getKeyspaces();
    }

    /**
     * Timer to be used for flush interval.
     *
     * @param config {@link IConfiguration} to get configuration details from priam.
     * @return the timer to be used for flush interval.
     *     <p>If {@link IConfiguration#getFlushSchedulerType()} is {@link
     *     com.netflix.priam.scheduler.SchedulerType#HOUR} then it expects {@link
     *     IConfiguration#getFlushInterval()} in the format of hour=x or daily=x
     *     <p>If {@link IConfiguration#getFlushSchedulerType()} is {@link
     *     com.netflix.priam.scheduler.SchedulerType#CRON} then it expects a valid CRON expression
     *     from {@link IConfiguration#getFlushCronExpression()}
     * @throws Exception if the configurations are wrong. .e.g invalid cron expression.
     */
    public static TaskTimer getTimer(IConfiguration config) throws Exception {

        CronTimer cronTimer = null;
        switch (config.getFlushSchedulerType()) {
            case HOUR:
                String timerVal = config.getFlushInterval(); // e.g. hour=0 or daily=10
                if (timerVal == null) return null;
                String s[] = timerVal.split("=");
                if (s.length != 2) {
                    throw new IllegalArgumentException(
                            "Flush interval format is invalid.  Expecting name=value, received: "
                                    + timerVal);
                }
                String name = s[0].toUpperCase();
                Integer time = new Integer(s[1]);
                switch (name) {
                    case "HOUR":
                        cronTimer =
                                new CronTimer(
                                        Task.FLUSH.name(), time, 0); // minute, sec after each hour
                        break;
                    case "DAILY":
                        cronTimer =
                                new CronTimer(
                                        Task.FLUSH.name(),
                                        time,
                                        0,
                                        0); // hour, minute, sec to run on a daily basis
                        break;
                    default:
                        throw new UnsupportedTypeException(
                                "Flush interval type is invalid.  Expecting \"hour, daily\", received: "
                                        + name);
                }

                break;
            case CRON:
                cronTimer =
                        CronTimer.getCronTimer(Task.FLUSH.name(), config.getFlushCronExpression());
                break;
        }
        return cronTimer;
    }
}
