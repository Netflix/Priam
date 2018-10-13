package com.netflix.priam.resources;

import static org.junit.Assert.*;

import com.netflix.priam.PriamServer;
import com.netflix.priam.config.FakeConfiguration;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMockit.class)
public class PriamConfigTest {
    private @Mocked PriamServer priamServer;

    private PriamConfig resource;

    private FakeConfiguration fakeConfiguration;

    @Before
    public void setUp() {
        resource = new PriamConfig(priamServer);
        fakeConfiguration = new FakeConfiguration("cass_test");
        fakeConfiguration.fakeProperties.put("test.prop", "test_value");
    }

    @Test
    public void getPriamConfig() {
        final Map<String, String> expected = new HashMap<>();
        expected.put("backupLocation", "casstestbackup");
        new Expectations() {
            {
                priamServer.getConfiguration();
                result = fakeConfiguration;
                times = 2;
            }
        };

        Response response = resource.getPriamConfigByName("all", "backupLocation");
        assertEquals(200, response.getStatus());
        assertEquals(expected, response.getEntity());

        Response badResponse = resource.getPriamConfigByName("all", "getUnrealThing");
        assertEquals(404, badResponse.getStatus());
    }

    @Test
    public void getProperty() {
        final Map<String, String> expected = new HashMap<>();
        expected.put("test.prop", "test_value");
        new Expectations() {
            {
                priamServer.getConfiguration();
                result = fakeConfiguration;
                times = 3;
            }
        };

        Response response = resource.getProperty("test.prop", null);
        assertEquals(200, response.getStatus());
        assertEquals(expected, response.getEntity());

        Response defaultResponse = resource.getProperty("not.a.property", "NOVALUE");
        expected.clear();
        expected.put("not.a.property", "NOVALUE");
        assertEquals(200, defaultResponse.getStatus());
        assertEquals(expected, defaultResponse.getEntity());

        Response badResponse = resource.getProperty("not.a.property", null);
        assertEquals(404, badResponse.getStatus());
    }
}
