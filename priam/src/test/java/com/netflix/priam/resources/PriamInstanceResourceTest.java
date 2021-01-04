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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableSet;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

public class PriamInstanceResourceTest {
    private static final String APP_NAME = "myApp";
    private static final int NODE_ID = 3;

    private @Mocked IConfiguration config;
    private @Mocked IPriamInstanceFactory factory;
    private @Mocked InstanceInfo instanceInfo;
    private PriamInstanceResource resource;

    @Before
    public void setUp() {
        resource = new PriamInstanceResource(config, factory, instanceInfo);
    }

    @Test
    public void getInstances(
            @Mocked final PriamInstance instance1,
            @Mocked final PriamInstance instance2,
            @Mocked final PriamInstance instance3) {
        new Expectations() {
            final ImmutableSet<PriamInstance> instances =
                    ImmutableSet.of(instance1, instance2, instance3);

            {
                config.getAppName();
                result = APP_NAME;
                factory.getAllIds(APP_NAME);
                result = instances;
                instance1.toString();
                result = "instance1";
                instance2.toString();
                result = "instance2";
                instance3.toString();
                result = "instance3";
            }
        };

        assertEquals("instance1\ninstance2\ninstance3\n", resource.getInstances());
    }

    @Test
    public void getInstance(@Mocked final PriamInstance instance) {
        final String expected = "plain text describing the instance";
        new Expectations() {
            {
                config.getAppName();
                result = APP_NAME;
                factory.getInstance(APP_NAME, instanceInfo.getRegion(), NODE_ID);
                result = instance;
                instance.toString();
                result = expected;
            }
        };

        assertEquals(expected, resource.getInstance(NODE_ID));
    }

    @Test
    public void getInstance_notFound() {
        new Expectations() {
            {
                config.getAppName();
                result = APP_NAME;
                factory.getInstance(APP_NAME, instanceInfo.getRegion(), NODE_ID);
                result = null;
            }
        };

        try {
            resource.getInstance(NODE_ID);
            fail("Expected WebApplicationException thrown");
        } catch (WebApplicationException e) {
            assertEquals(404, e.getResponse().getStatus());
            assertEquals(
                    "No priam instance with id " + NODE_ID + " found", e.getResponse().getEntity());
        }
    }

    @Test
    public void createInstance(@Mocked final PriamInstance instance) {
        final String instanceID = "i-abc123";
        final String hostname = "dom.com";
        final String ip = "123.123.123.123";
        final String rack = "us-east-1a";
        final String token = "1234567890";

        new Expectations() {
            {
                config.getAppName();
                result = APP_NAME;
                factory.create(APP_NAME, NODE_ID, instanceID, hostname, ip, rack, null, token);
                result = instance;
                instance.getId();
                result = NODE_ID;
            }
        };

        Response response = resource.createInstance(NODE_ID, instanceID, hostname, ip, rack, token);
        assertEquals(201, response.getStatus());
        assertEquals("/" + NODE_ID, response.getMetadata().getFirst("location").toString());
    }

    @Test
    public void deleteInstance(@Mocked final PriamInstance instance) {
        new Expectations() {
            {
                config.getAppName();
                result = APP_NAME;
                factory.getInstance(APP_NAME, instanceInfo.getRegion(), NODE_ID);
                result = instance;
                factory.delete(instance);
            }
        };

        Response response = resource.deleteInstance(NODE_ID);
        assertEquals(204, response.getStatus());
    }

    @Test
    public void deleteInstance_notFound() {
        new Expectations() {
            {
                config.getAppName();
                result = APP_NAME;
                factory.getInstance(APP_NAME, instanceInfo.getRegion(), NODE_ID);
                result = null;
            }
        };

        try {
            resource.getInstance(NODE_ID);
            fail("Expected WebApplicationException thrown");
        } catch (WebApplicationException e) {
            assertEquals(404, e.getResponse().getStatus());
            assertEquals(
                    "No priam instance with id " + NODE_ID + " found", e.getResponse().getEntity());
        }
    }
}
