package com.netflix.priam.resources;

import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;

import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(JMockit.class)
public class PriamInstanceResourceTest
{
    private static final String APP_NAME = "myApp";
    private static final int NODE_ID = 3;

    private @Mocked IConfiguration config;
    private @Mocked IPriamInstanceFactory factory;
    private PriamInstanceResource resource;

    @Before
    public void setUp()
    {
        resource = new PriamInstanceResource(config, factory);
    }

    @Test
    public void getInstances(@Mocked final PriamInstance instance1, @Mocked final PriamInstance instance2, @Mocked final PriamInstance instance3)
    {
        new Expectations() {
            List<PriamInstance> instances = ImmutableList.of(instance1, instance2, instance3);

            {
                config.getAppName(); result = APP_NAME;
                factory.getAllIds(APP_NAME); result = instances;
                instance1.toString(); result = "instance1";
                instance2.toString(); result = "instance2";
                instance3.toString(); result = "instance3";
            }
        };

        assertEquals("instance1\ninstance2\ninstance3\n", resource.getInstances());
    }

    @Test
    public void getInstance(@Mocked final PriamInstance instance)
    {
        final String expected = "plain text describing the instance";
        new Expectations() {
            {
                config.getAppName(); result = APP_NAME;
                factory.getInstance(APP_NAME, config.getDC(), NODE_ID); result = instance;
                instance.toString(); result = expected;
            }
        };

        assertEquals(expected, resource.getInstance(NODE_ID));
    }

    @Test
    public void getInstance_notFound()
    {
        new Expectations() {{
            config.getAppName(); result = APP_NAME;
            factory.getInstance(APP_NAME, config.getDC(), NODE_ID); result = null;
        }};

        try
        {
            resource.getInstance(NODE_ID);
            fail("Expected WebApplicationException thrown");
        } catch(WebApplicationException e)
        {
            assertEquals(404, e.getResponse().getStatus());
            assertEquals("No priam instance with id " + NODE_ID + " found", e.getResponse().getEntity());
        }
    }

    @Test
    public void createInstance(@Mocked final PriamInstance instance)
    {
        final String instanceID = "i-abc123";
        final String hostname = "dom.com";
        final String ip = "123.123.123.123";
        final String rack = "us-east-1a";
        final String token = "1234567890";

        new Expectations() {
          {
              config.getAppName(); result = APP_NAME;
              factory.create(APP_NAME, NODE_ID, instanceID, hostname, ip, rack, null, token); result = instance;
              instance.getId(); result = NODE_ID;
          }
        };

        Response response = resource.createInstance(NODE_ID, instanceID, hostname, ip, rack, token);
        assertEquals(201, response.getStatus());
        assertEquals("/"+NODE_ID, response.getMetadata().getFirst("location").toString());
    }

    @Test
    public void deleteInstance(@Mocked final PriamInstance instance)
    {
        new Expectations() {
          {
              config.getAppName(); result = APP_NAME;
              factory.getInstance(APP_NAME, config.getDC(), NODE_ID); result = instance;
              factory.delete(instance);
          }
        };

        Response response = resource.deleteInstance(NODE_ID);
        assertEquals(204, response.getStatus());
    }

    @Test
    public void deleteInstance_notFound()
    {
        new Expectations() {{
            config.getAppName(); result = APP_NAME;
            factory.getInstance(APP_NAME, config.getDC(), NODE_ID); result = null;
        }};

        try
        {
            resource.getInstance(NODE_ID);
            fail("Expected WebApplicationException thrown");
        } catch(WebApplicationException e)
        {
            assertEquals(404, e.getResponse().getStatus());
            assertEquals("No priam instance with id " + NODE_ID + " found", e.getResponse().getEntity());
        }
    }
}