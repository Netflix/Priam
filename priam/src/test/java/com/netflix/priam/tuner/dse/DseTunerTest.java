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

import com.google.common.io.Files;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.dse.DseConfigStub;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DseTunerTest {
    private IConfiguration config;
    private DseConfigStub dseConfig;
    private AuditLogTunerYaml auditLogTunerYaml;
    private AuditLogTunerLog4J auditLogTunerLog4j;
    private File targetFile;
    private File targetDseYamlFile;

    @Before
    public void setup() throws IOException {
        config = new FakeConfiguration();
        dseConfig = new DseConfigStub();
        auditLogTunerYaml = new AuditLogTunerYaml(dseConfig);
        auditLogTunerLog4j = new AuditLogTunerLog4J(config, dseConfig);

        File targetDir = new File(config.getCassHome() + "/conf");
        if (!targetDir.exists()) targetDir.mkdirs();

        targetFile = new File(config.getCassHome() + AuditLogTunerLog4J.AUDIT_LOG_FILE);
        Files.copy(new File("src/test/resources/" + AuditLogTunerLog4J.AUDIT_LOG_FILE), targetFile);
    }

    @Test
    public void auditLogProperties_Enabled() throws IOException {
        dseConfig.setAuditLogEnabled(true);
        auditLogTunerLog4j.tuneAuditLog();

        Properties p = new Properties();
        p.load(new FileReader(targetFile));
        Assert.assertTrue(p.containsKey(AuditLogTunerLog4J.PRIMARY_AUDIT_LOG_ENTRY));
    }

    @Test
    public void auditLogProperties_Disabled() throws IOException {
        dseConfig.setAuditLogEnabled(false);
        auditLogTunerLog4j.tuneAuditLog();

        Properties p = new Properties();
        p.load(new FileReader(targetFile));
        Assert.assertFalse(p.containsKey(AuditLogTunerLog4J.PRIMARY_AUDIT_LOG_ENTRY));
    }

    /**
     * This is different because we test the disabled step using the already used enabled file (not
     * a clean copy over of the original props file from the resources dir), and vice versa
     *
     * @throws IOException
     */
    @Test
    public void auditLogProperties_ThereAndBackAgain() throws IOException {
        auditLogProperties_Enabled();
        auditLogProperties_Disabled();
        auditLogProperties_Enabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
    }

    @Test
    public void auditLogYamlProperties_Enabled() throws IOException {
        File targetDseDir = new File(config.getCassHome() + "/resources/dse/conf/");
        if (!targetDseDir.exists()) {
            targetDseDir.mkdirs();
        }

        int index = dseConfig.getDseYamlLocation().lastIndexOf('/') + 1;
        targetDseYamlFile =
                new File(targetDseDir + dseConfig.getDseYamlLocation().substring(index - 1));
        Files.copy(
                new File(
                        "src/test/resources/conf/"
                                + dseConfig.getDseYamlLocation().substring(index)),
                targetDseYamlFile);

        dseConfig.setAuditLogEnabled(true);
        auditLogTunerYaml.tuneAuditLog();
    }

    @Test
    public void auditLogYamlProperties_Disabled() throws IOException {
        File targetDseDir = new File(config.getCassHome() + "/resources/dse/conf/");
        if (!targetDseDir.exists()) {
            targetDseDir.mkdirs();
        }

        int index = dseConfig.getDseYamlLocation().lastIndexOf('/') + 1;
        targetDseYamlFile =
                new File(targetDseDir + dseConfig.getDseYamlLocation().substring(index - 1));
        Files.copy(
                new File(
                        "src/test/resources/conf/"
                                + dseConfig.getDseYamlLocation().substring(index)),
                targetDseYamlFile);

        dseConfig.setAuditLogEnabled(false);
        auditLogTunerYaml.tuneAuditLog();
    }
}
