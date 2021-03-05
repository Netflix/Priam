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
package com.netflix.priam.resources;

import static org.junit.Assert.*;

import com.netflix.priam.PriamServer;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

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
        new Expectations() {
            {
                priamServer.getConfiguration();
                result = fakeConfiguration;
                times = 3;
            }
        };

        Response response = resource.getPriamConfig("all");
        assertEquals(200, response.getStatus());

        Map<String, Object> result =
                GsonJsonSerializer.getGson().fromJson(response.getEntity().toString(), Map.class);
        assertNotNull(result);
        assertTrue(!result.isEmpty());

        final Map<String, String> expected = new HashMap<>();
        expected.put("backupLocation", "casstestbackup");
        String expectedJsonString = GsonJsonSerializer.getGson().toJson(expected);
        response = resource.getPriamConfigByName("all", "backupLocation");
        assertEquals(200, response.getStatus());
        assertEquals(expectedJsonString, response.getEntity());
        result = GsonJsonSerializer.getGson().fromJson(response.getEntity().toString(), Map.class);
        assertEquals(result, expected);

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

        String expectedJsonString = GsonJsonSerializer.getGson().toJson(expected);
        Response response = resource.getProperty("test.prop", null);
        assertEquals(200, response.getStatus());
        assertEquals(expectedJsonString, response.getEntity());

        Map<String, Object> result =
                GsonJsonSerializer.getGson().fromJson(response.getEntity().toString(), Map.class);
        assertNotNull(result);
        assertTrue(!result.isEmpty());

        Response defaultResponse = resource.getProperty("not.a.property", "NOVALUE");
        expected.clear();
        expected.put("not.a.property", "NOVALUE");
        expectedJsonString = GsonJsonSerializer.getGson().toJson(expected);
        assertEquals(200, defaultResponse.getStatus());
        assertEquals(expectedJsonString, defaultResponse.getEntity());
        result =
                GsonJsonSerializer.getGson()
                        .fromJson(defaultResponse.getEntity().toString(), Map.class);
        assertEquals(result, expected);

        Response badResponse = resource.getProperty("not.a.property", null);
        assertEquals(404, badResponse.getStatus());
    }
}
