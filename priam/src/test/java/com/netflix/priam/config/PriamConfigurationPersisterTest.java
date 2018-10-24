package com.netflix.priam.config;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.TestModule;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PriamConfigurationPersisterTest {
    private static PriamConfigurationPersister persister;

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private FakeConfiguration fakeConfiguration;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new TestModule());
        fakeConfiguration = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        fakeConfiguration.fakeProperties.put("priam_test_config", folder.getRoot().getPath());

        if (persister == null) persister = injector.getInstance(PriamConfigurationPersister.class);
    }

    @After
    public void cleanUp() {
        fakeConfiguration.fakeProperties.clear();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void execute() throws Exception {
        Path structuredJson = Paths.get(folder.getRoot().getPath(), "structured.json");

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
