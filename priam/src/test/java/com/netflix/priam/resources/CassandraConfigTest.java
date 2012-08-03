package com.netflix.priam.resources;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import com.netflix.priam.PriamServer;
import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;

import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CassandraConfigTest
{
    private @Mocked PriamServer priamServer;
    private @Mocked DoubleRing doubleRing;
    private CassandraConfig resource;

    @Before
    public void setUp()
    {
        resource = new CassandraConfig(priamServer, doubleRing);
    }

    @Test
    public void getSeeds() throws Exception
    {
        final List<String> seeds = ImmutableList.of("seed1", "seed2", "seed3");
        new NonStrictExpectations() {
            InstanceIdentity identity;

            {
                priamServer.getId(); result = identity; times = 2;
                identity.getSeeds(); result = seeds; times = 2;
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
                priamServer.getId(); result = identity; times = 2;
                identity.getSeeds(); result = seeds; times = 2;
            }
        };

        Response response = resource.getSeeds();
        assertEquals(404, response.getStatus());
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

    @Test
    public void getToken()
    {
        final String token = "myToken";
        new NonStrictExpectations() {
            InstanceIdentity identity;
            PriamInstance instance;

            {
                priamServer.getId(); result = identity; times = 2;
                identity.getInstance(); result = instance; times = 2;
                instance.getToken(); result = token; times = 2;
            }
        };

        Response response = resource.getToken();
        assertEquals(200, response.getStatus());
        assertEquals(token, response.getEntity());
    }

    @Test
    public void getToken_notFound()
    {
        final String token = "";
        new NonStrictExpectations() {
            InstanceIdentity identity;
            PriamInstance instance;

            {
                priamServer.getId(); result = identity;
                identity.getInstance(); result = instance;
                instance.getToken(); result = token;
            }
        };

        Response response = resource.getToken();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void getToken_handlesException()
    {
        new NonStrictExpectations() {
            InstanceIdentity identity;
            PriamInstance instance;

            {
                priamServer.getId(); result = identity;
                identity.getInstance(); result = instance;
                instance.getToken(); result = new RuntimeException();
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

    @Test
    public void doubleRing() throws Exception
    {
        new NonStrictExpectations() {{
            doubleRing.backup();
            doubleRing.doubleSlots();
        }};

        Response response = resource.doubleRing();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void doubleRing_ioExceptionInBackup() throws Exception
    {
        final IOException exception = new IOException();
        new NonStrictExpectations() {{
            doubleRing.backup(); result = exception;
            doubleRing.restore();
        }};

        try
        {
          resource.doubleRing();
          fail("Excepted RuntimeException");
        }
        catch (RuntimeException e)
        {
          assertEquals(exception, e.getCause());
        }
    }

    @Test(expected=IOException.class)
    public void doubleRing_ioExceptionInRestore() throws Exception
    {
        new NonStrictExpectations() {{
            doubleRing.backup(); result = new IOException();
            doubleRing.restore(); result = new IOException();
        }};

        resource.doubleRing();
    }

    @Test(expected=ClassNotFoundException.class)
    public void doubleRing_classNotFoundExceptionInRestore() throws Exception
    {
        new NonStrictExpectations() {{
            doubleRing.backup(); result = new IOException();
            doubleRing.restore(); result = new ClassNotFoundException();
        }};

        resource.doubleRing();
    }
}