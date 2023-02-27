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
package com.netflix.priam.tuner.dse;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraProcessManager;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.merics.CassMonitorMetrics;
import com.netflix.priam.tuner.dse.IDseConfiguration.NodeType;
import java.util.Map;
import javax.inject.Inject;

public class DseProcessManager extends CassandraProcessManager {
    private final IDseConfiguration dseConfig;
    private InstanceState instanceState;

    @Inject
    public DseProcessManager(
            IConfiguration config,
            IDseConfiguration dseConfig,
            InstanceState instanceState,
            CassMonitorMetrics cassMonitorMetrics) {
        super(config, instanceState, cassMonitorMetrics);
        this.dseConfig = dseConfig;
    }

    protected void setEnv(Map<String, String> env) {
        super.setEnv(env);

        NodeType nodeType = dseConfig.getNodeType();
        if (nodeType == NodeType.ANALYTIC_HADOOP) env.put("CLUSTER_TYPE", "-t");
        else if (nodeType == NodeType.ANALYTIC_SPARK) env.put("CLUSTER_TYPE", "-k");
        else if (nodeType == NodeType.ANALYTIC_HADOOP_SPARK) env.put("CLUSTER_TYPE", "-k -t");
        else if (nodeType == NodeType.SEARCH) env.put("CLUSTER_TYPE", "-s");
    }
}
