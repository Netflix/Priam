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

import static com.netflix.priam.tuner.dse.IDseConfiguration.NodeType;
import static org.apache.cassandra.locator.SnitchProperties.RACKDC_PROPERTY_FILENAME;

import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.tuner.StandardTuner;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.Properties;
import javax.inject.Inject;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes Datastax Enterprise-specific changes to the c* yaml and dse-yaml.
 *
 * @author jason brown
 * @author minh do
 */
public class DseTuner extends StandardTuner {
    private static final Logger logger = LoggerFactory.getLogger(DseTuner.class);
    private final IDseConfiguration dseConfig;
    private final IAuditLogTuner auditLogTuner;

    @Inject
    public DseTuner(
            IConfiguration config,
            IBackupRestoreConfig backupRestoreConfig,
            IDseConfiguration dseConfig,
            IAuditLogTuner auditLogTuner,
            InstanceInfo instanceInfo) {
        super(config, backupRestoreConfig, instanceInfo);
        this.dseConfig = dseConfig;
        this.auditLogTuner = auditLogTuner;
    }

    public void writeAllProperties(String yamlLocation, String hostname, String seedProvider)
            throws Exception {
        super.writeAllProperties(yamlLocation, hostname, seedProvider);
        writeCassandraSnitchProperties();
        auditLogTuner.tuneAuditLog();
    }

    private void writeCassandraSnitchProperties() {
        final NodeType nodeType = dseConfig.getNodeType();
        if (nodeType == NodeType.REAL_TIME_QUERY) return;

        Reader reader = null;
        try {
            String filePath = config.getCassHome() + "/conf/" + RACKDC_PROPERTY_FILENAME;
            reader = new FileReader(filePath);
            Properties properties = new Properties();
            properties.load(reader);
            String suffix = "";
            if (nodeType == NodeType.SEARCH) suffix = "_solr";
            if (nodeType == NodeType.ANALYTIC_HADOOP) suffix = "_hadoop";
            if (nodeType == NodeType.ANALYTIC_HADOOP_SPARK) suffix = "_hadoop_spark";
            if (nodeType == NodeType.ANALYTIC_SPARK) suffix = "_spark";

            properties.put("dc_suffix", suffix);
            properties.store(new FileWriter(filePath), "");
        } catch (Exception e) {
            throw new RuntimeException("Unable to read " + RACKDC_PROPERTY_FILENAME, e);
        } finally {
            FileUtils.closeQuietly(reader);
        }
    }

    protected String getSnitch() {
        return dseConfig.getDseDelegatingSnitch();
    }
}
