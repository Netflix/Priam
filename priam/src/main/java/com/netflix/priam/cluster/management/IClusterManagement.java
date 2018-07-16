/**
 * Copyright 2017 Netflix, Inc.
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
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.JMXConnectionException;
import com.netflix.priam.utils.JMXConnectorMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by vinhn on 10/12/16.
 */
public abstract class IClusterManagement<T> extends Task {
    public enum Task {FLUSH, COMPACTION};
    private static final Logger logger = LoggerFactory.getLogger(IClusterManagement.class);
    private IMetricPublisher metricPublisher;
    private Task taskType;
    private IMeasurement measurement;

    protected IClusterManagement(IConfiguration config, Task taskType, IMetricPublisher metricPublisher, IMeasurement measurement) {
        super(config);
        this.taskType = taskType;
        this.metricPublisher = metricPublisher;
        this.measurement = measurement;
    }

    @Override
    public void execute() throws JMXConnectionException, Exception {
        List<T> result = null;

        try(JMXConnectorMgr connMgr = new JMXConnectorMgr(config)) {
            result = runTask(connMgr);
            measurement.incrementSuccessCnt(1);
            this.metricPublisher.publish(measurement); //signal that there was a success
            logger.info("Successfully finished executing the cluster management task: {} with result: {}", taskType, result);
        } catch (IOException | InterruptedException e){
            measurement.incrementFailureCnt(1);
            this.metricPublisher.publish(measurement); //signal that there was a failure
            throw new JMXConnectionException("Exception during creating JMX connection for operation: " + taskType.name(), e);
        }catch (Exception e) {
            measurement.incrementFailureCnt(1);
            this.metricPublisher.publish(measurement); //signal that there was a failure
            throw new RuntimeException(String.format("Exception during %s.",taskType), e);

        }

        if (result.isEmpty()) {
            logger.warn("{} task completed successfully but no action was done.", taskType.name());
        }
    }

    @Override
    public String getName() {
        return taskType.name();
    }

    protected abstract List<T> runTask(JMXConnectorMgr jmxConnectorMgr) throws TaskException;
}
