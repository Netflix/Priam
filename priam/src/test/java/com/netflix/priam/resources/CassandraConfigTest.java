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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.merics.CassMonitorMetrics;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import javax.ws.rs.core.Response;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

public class CassandraConfigTest {
    private @Mocked PriamServer priamServer;
    private @Mocked DoubleRing doubleRing;
    private CassandraConfig resource;
    private InstanceIdentity instanceIdentity;

    @Before
    public void setUp() {
        CassMonitorMetrics cassMonitorMetrics =
                Guice.createInjector(new BRTestModule()).getInstance(CassMonitorMetrics.class);
        instanceIdentity =
                Guice.createInjector(new BRTestModule()).getInstance(InstanceIdentity.class);
        resource = new CassandraConfig(priamServer, doubleRing, cassMonitorMetrics);
    }

    @Test
    public void getSeeds(@Mocked final InstanceIdentity identity) throws Exception {
        final List<String> seeds = ImmutableList.of("seed1", "seed2", "seed3");
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                times = 1;
                identity.getSeeds();
                result = seeds;
                times = 1;
            }
        };

        Response response = resource.getSeeds();
        assertEquals(200, response.getStatus());
        assertEquals("seed1,seed2,seed3", response.getEntity());
    }

    @Test
    public void getSeeds_notFound(@Mocked final InstanceIdentity identity) throws Exception {
        final List<String> seeds = ImmutableList.of();
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                times = 1;
                identity.getSeeds();
                result = seeds;
                times = 1;
            }
        };

        Response response = resource.getSeeds();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void getSeeds_handlesUnknownHostException(@Mocked final InstanceIdentity identity)
            throws Exception {
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                identity.getSeeds();
                result = new UnknownHostException();
            }
        };

        Response response = resource.getSeeds();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void getToken(
            @Mocked final InstanceIdentity identity, @Mocked final PriamInstance instance) {
        final String token = "myToken";
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                times = 2;
                identity.getInstance();
                result = instance;
                times = 2;
                instance.getToken();
                result = token;
                times = 2;
            }
        };

        Response response = resource.getToken();
        assertEquals(200, response.getStatus());
        assertEquals(token, response.getEntity());
    }

    @Test
    public void getToken_notFound(
            @Mocked final InstanceIdentity identity, @Mocked final PriamInstance instance) {
        final String token = "";
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                identity.getInstance();
                result = instance;
                instance.getToken();
                result = token;
            }
        };

        Response response = resource.getToken();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void getToken_handlesException(
            @Mocked final InstanceIdentity identity, @Mocked final PriamInstance instance) {
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                identity.getInstance();
                result = instance;
                instance.getToken();
                result = new RuntimeException();
            }
        };

        Response response = resource.getToken();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void isReplaceToken(@Mocked final InstanceIdentity identity) {
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                identity.isReplace();
                result = true;
            }
        };

        Response response = resource.isReplaceToken();
        assertEquals(200, response.getStatus());
        assertEquals("true", response.getEntity());
    }

    @Test
    public void isReplaceToken_handlesException(@Mocked final InstanceIdentity identity) {
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                identity.isReplace();
                result = new RuntimeException();
            }
        };

        Response response = resource.isReplaceToken();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void getReplacedAddress(@Mocked final InstanceIdentity identity) {
        final String replacedIp = "127.0.0.1";
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = identity;
                identity.getReplacedIp();
                result = replacedIp;
            }
        };

        Response response = resource.getReplacedIp();
        assertEquals(200, response.getStatus());
        assertEquals(replacedIp, response.getEntity());
    }

    @Test
    public void setReplacedIp() {
        new Expectations() {
            {
                priamServer.getInstanceIdentity();
                result = instanceIdentity;
            }
        };

        Response response = resource.setReplacedIp("127.0.0.1");
        assertEquals(200, response.getStatus());
        assertEquals("127.0.0.1", instanceIdentity.getReplacedIp());
        assertTrue(instanceIdentity.isReplace());

        response = resource.setReplacedIp(null);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void doubleRing() throws Exception {
        new Expectations() {
            {
                doubleRing.backup();
                doubleRing.doubleSlots();
            }
        };

        Response response = resource.doubleRing();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void doubleRing_ioExceptionInBackup() throws Exception {
        final IOException exception = new IOException();
        new Expectations() {
            {
                doubleRing.backup();
                result = exception;
                doubleRing.restore();
            }
        };

        try {
            resource.doubleRing();
            fail("Excepted RuntimeException");
        } catch (RuntimeException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test(expected = IOException.class)
    public void doubleRing_ioExceptionInRestore() throws Exception {
        new Expectations() {
            {
                doubleRing.backup();
                result = new IOException();
                doubleRing.restore();
                result = new IOException();
            }
        };

        resource.doubleRing();
    }

    @Test(expected = ClassNotFoundException.class)
    public void doubleRing_classNotFoundExceptionInRestore() throws Exception {
        new Expectations() {
            {
                doubleRing.backup();
                result = new IOException();
                doubleRing.restore();
                result = new ClassNotFoundException();
            }
        };

        resource.doubleRing();
    }
}
