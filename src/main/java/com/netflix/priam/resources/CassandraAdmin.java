package com.netflix.priam.resources;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.SystemUtils;

/**
 * Do general operations. Start/Stop and some JMX node tool commands
 */
@Path("/v1/cassadmin")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraAdmin
{
    private static final String REST_HEADER_KEYSPACES = "keyspaces";
    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final Logger logger = LoggerFactory.getLogger(CassandraAdmin.class);
    private IConfiguration config;

    @Inject
    public CassandraAdmin(IConfiguration config)
    {
        this.config = config;
    }

    @GET
    @Path("/start")
    public Response cassStart() throws IOException, InterruptedException, JSONException
    {
        SystemUtils.startCassandra(true, config);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/stop")
    public Response cassStop() throws IOException, InterruptedException, JSONException
    {
        SystemUtils.stopCassandra(config);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/refresh")
    public Response cassRefresh(@QueryParam(REST_HEADER_KEYSPACES) String keyspaces) throws IOException, ExecutionException, InterruptedException, JSONException
    {
        logger.info("node tool refresh is being called");
        if (StringUtils.isBlank(keyspaces))
            return Response.status(400).entity("Missing keyspace in request").build();

        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        nodetool.refresh(Lists.newArrayList(keyspaces.split(",")));
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/info")
    public Response cassInfo() throws IOException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        logger.info("node tool info being called");
        return Response.ok(nodetool.info(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/ring")
    public Response cassRing() throws IOException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        logger.info("node tool ring being called");
        return Response.ok(nodetool.ring(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/flush")
    public Response cassFlush() throws IOException, InterruptedException, ExecutionException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        logger.info("node tool flush being called");
        nodetool.flush();
        return Response.ok().build();
    }

    @GET
    @Path("/compact")
    public Response cassCompact() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        logger.info("node tool compact being called");
        nodetool.compact();
        return Response.ok().build();
    }

    @GET
    @Path("/cleanup")
    public Response cassCleanup() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        logger.info("node tool cleanup being called");
        nodetool.cleanup();
        return Response.ok().build();
    }

    @GET
    @Path("/repair")
    public Response cassRepair() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        logger.info("node tool repair being called");
        nodetool.repair();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }
}
