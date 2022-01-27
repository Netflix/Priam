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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.TestModule;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PriamConfigurationPersisterTest {
    private static PriamConfigurationPersister persister;

    @TempDir public File folder;

    private FakeConfiguration fakeConfiguration;

    @BeforeEach
    public void setUp() {
        Injector injector = Guice.createInjector(new TestModule());
        fakeConfiguration = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        fakeConfiguration.fakeProperties.put("priam_test_config", folder.getPath());

        if (persister == null) persister = injector.getInstance(PriamConfigurationPersister.class);
    }

    @AfterEach
    public void cleanUp() {
        fakeConfiguration.fakeProperties.clear();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void execute() throws Exception {
        Path structuredJson = Paths.get(folder.getPath(), "structured.json");

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
