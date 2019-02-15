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
package com.netflix.priam.config;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.backup.BRTestModule;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ArchaiusModule.class, BRTestModule.class})
public class PriamConfigurationPersisterTest {
    @Inject private PriamConfigurationPersister persister;
    @Inject private IConfiguration fakeConfiguration;

    @Test
    @SuppressWarnings("unchecked")
    public void execute() throws Exception {
        Path structuredJson =
                Paths.get(fakeConfiguration.getMergedConfigurationDirectory(), "structured.json");

        persister.execute();
        assertTrue(structuredJson.toFile().exists());

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> myMap =
                objectMapper.readValue(Files.readAllBytes(structuredJson), HashMap.class);
        assertEquals(myMap.get("backupLocation"), fakeConfiguration.getBackupLocation());
    }

    @Test
    public void getTimer() {
        assertEquals(
                "0 * * * * ? *",
                PriamConfigurationPersister.getTimer(fakeConfiguration).getCronExpression());
    }
}
