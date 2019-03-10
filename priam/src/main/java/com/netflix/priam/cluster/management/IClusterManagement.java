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
import com.netflix.priam.health.CassandraMonitor;
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.scheduler.Task;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by vinhn on 10/12/16. */
public abstract class IClusterManagement<T> extends Task {
    public enum Task {
        FLUSH,
        COMPACTION
    }

    private static final Logger logger = LoggerFactory.getLogger(IClusterManagement.class);
    private final Task taskType;
    private final IMeasurement measurement;
    private static final Lock lock = new ReentrantLock();

    protected IClusterManagement(IConfiguration config, Task taskType, IMeasurement measurement) {
        super(config);
        this.taskType = taskType;
        this.measurement = measurement;
    }

    @Override
    public void execute() throws Exception {
        if (!CassandraMonitor.hasCassadraStarted()) {
            logger.debug("Cassandra has not started, hence {} will not run", taskType);
            return;
        }

        if (!lock.tryLock()) {
            logger.error("Operation is already running! Try again later.");
            throw new Exception("Operation already running");
        }

        try {
            String result = runTask();
            measurement.incrementSuccess();
            logger.info(
                    "Successfully finished executing the cluster management task: {} with result: {}",
                    taskType,
                    result);
            if (result.isEmpty()) {
                logger.warn(
                        "{} task completed successfully but no action was done.", taskType.name());
            }
        } catch (Exception e) {
            measurement.incrementFailure();
            throw new Exception("Exception during execution of operation: " + taskType.name(), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String getName() {
        return taskType.name();
    }

    protected abstract String runTask() throws Exception;
}
