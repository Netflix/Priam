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

package com.netflix.priam.tuner.dse;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/** Dse tuner for audit log via YAML. Use this for DSE version 4.x Created by aagrawal on 8/8/17. */
public class AuditLogTunerYaml implements IAuditLogTuner {

    private final IDseConfiguration dseConfig;
    private static final String AUDIT_LOG_DSE_ENTRY = "audit_logging_options";
    private static final Logger logger = LoggerFactory.getLogger(AuditLogTunerYaml.class);

    @Inject
    public AuditLogTunerYaml(IDseConfiguration dseConfig) {
        this.dseConfig = dseConfig;
    }

    public void tuneAuditLog() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        String dseYaml = dseConfig.getDseYamlLocation();
        try {
            Map<String, Object> map = yaml.load(new FileInputStream(dseYaml));

            if (map.containsKey(AUDIT_LOG_DSE_ENTRY)) {
                Boolean isEnabled =
                        (Boolean)
                                ((Map<String, Object>) map.get(AUDIT_LOG_DSE_ENTRY)).get("enabled");

                // Enable/disable audit logging (need this in addition to log4j-server.properties
                // settings)
                if (dseConfig.isAuditLogEnabled()) {
                    if (!isEnabled) {
                        ((Map<String, Object>) map.get(AUDIT_LOG_DSE_ENTRY)).put("enabled", true);
                    }
                } else if (isEnabled) {
                    ((Map<String, Object>) map.get(AUDIT_LOG_DSE_ENTRY)).put("enabled", false);
                }
            }

            if (logger.isInfoEnabled()) {
                logger.info("Updating dse-yaml:\n" + yaml.dump(map));
            }
            yaml.dump(map, new FileWriter(dseYaml));
        } catch (FileNotFoundException fileNotFound) {
            logger.error(
                    "FileNotFound while trying to read yaml audit log for tuning: {}", dseYaml);
        } catch (IOException e) {
            logger.error(
                    "IOException while trying to write yaml file for audit log tuning: {}",
                    dseYaml);
        }
    }
}
