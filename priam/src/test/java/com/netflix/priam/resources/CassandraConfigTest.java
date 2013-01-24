package com.netflix.priam.resources;

import java.net.UnknownHostException;
import java.util.List;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.priam.PriamServer;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import static org.junit.Assert.assertEquals;

public class CassandraConfigTest
{
    private @Mocked PriamServer priamServer;
    private CassandraConfig resource;

    @Before
    public void setUp()
    {
        resource = new CassandraConfig(priamServer);
    }

    @Test
    public void getSeeds() throws Exception
    {
        final List<String> seeds = ImmutableList.of("seed1", "seed2", "seed3");
        new NonStrictExpectations() {
            InstanceIdentity identity;

            {
                priamServer.getId(); result = identity; times = 1;
                identity.getSeeds(); result = seeds; times = 1;
            }
        };

        Response response = resource.getSeeds();
        assertEquals(200, response.getStatus());
        assertEquals("seed1,seed2,seed3", response.getEntity());
    }

    @Test
    public void getSeeds_notFound() throws Exception
    {
        final List<String> seeds = ImmutableList.of();
        new NonStrictExpectations() {
            InstanceIdentity identity;

            {
                priamServer.getId(); result = identity; times = 1;
                identity.getSeeds(); result = seeds; times = 1;
            }
        };

        Response response = resource.getSeeds();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void getSeeds_handlesUnknownHostException() throws Exception
    {
        new Expectations() {
            InstanceIdentity identity;

            {
                priamServer.getId(); result = identity;
                identity.getSeeds(); result = new UnknownHostException();
            }
        };

        Response response = resource.getSeeds();
        assertEquals(500, response.getStatus());
    }

    @Ignore
    public void getToken()
    {
        final String token = "myToken";
        new NonStrictExpectations() {
            InstanceIdentity identity;
            PriamInstance instance;

            {
                priamServer.getId(); result = identity; times = 2;
                identity.getInstance(); result = instance; times = 2;
            }
        };

        Response response = resource.getToken();
        assertEquals(200, response.getStatus());
        assertEquals(token, response.getEntity());
    }

    @Ignore
    public void getToken_notFound()
    {
        final String token = "";
        new NonStrictExpectations() {
            InstanceIdentity identity;
            PriamInstance instance;

            {
                priamServer.getId(); result = identity;
                identity.getInstance(); result = instance;
            }
        };

        Response response = resource.getToken();
        assertEquals(500, response.getStatus());
    }

    @Ignore
    public void getToken_handlesException()
    {
        new NonStrictExpectations() {
            InstanceIdentity identity;
            PriamInstance instance;

            {
                priamServer.getId(); result = identity;
                identity.getInstance(); result = instance;
            }
        };

        Response response = resource.getToken();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void isReplaceToken()
    {
        new NonStrictExpectations() {
            InstanceIdentity identity;

            {
                priamServer.getId(); result = identity;
                identity.isReplace(); result = true;
            }
        };

        Response response = resource.isReplaceToken();
        assertEquals(200, response.getStatus());
        assertEquals("true", response.getEntity());
    }

    @Test
    public void isReplaceToken_handlesException()
    {
        new Expectations() {
            InstanceIdentity identity;

            {
                priamServer.getId(); result = identity;
                identity.isReplace(); result = new RuntimeException();
            }
        };

        Response response = resource.isReplaceToken();
        assertEquals(500, response.getStatus());
    }
}
