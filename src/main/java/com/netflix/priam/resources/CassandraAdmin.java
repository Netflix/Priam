package com.netflix.priam.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
@Path("/v1/cassadmin/{action}")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraAdmin
{

    private static final String REST_HEADER_ACTION = "action";
    private static final String REST_HEADER_CALLBACK = "callback";
    private static final String REST_HEADER_VALUE = "value";
    private static final Logger logger = LoggerFactory.getLogger(CassandraAdmin.class);
    private IConfiguration config;

    @Inject
    public CassandraAdmin(IConfiguration config)
    {
        this.config = config;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response runAction(@PathParam(REST_HEADER_ACTION) String action, @QueryParam(REST_HEADER_VALUE) String value, @QueryParam(REST_HEADER_CALLBACK) String callback)
    {
        try
        {
            logger.info("Trying to run command: " + action);

            if (action.equalsIgnoreCase("start"))
            {
                SystemUtils.startCassandra(true, config);
            }
            else if (action.equalsIgnoreCase("stop"))
            {
                SystemUtils.stopCassandra(config);
            }
            else if (action.equalsIgnoreCase("info"))
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                logger.info("node tool info being called");
                return buildResponse(callback, nodetool.info().toJSONString(), 200);
            }
            else if (action.equalsIgnoreCase("ring"))
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                logger.info("node tool ring being called");
                return buildResponse(callback, nodetool.ring().toJSONString(), 200);
            }
            else if (action.equalsIgnoreCase("flush"))
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                logger.info("node tool flush being called");
                nodetool.flush();
                return buildResponse(callback, "", 200);
            }
            else if (action.equalsIgnoreCase("compact"))
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                logger.info("node tool compaction being called");
                nodetool.compact();
                return buildResponse(callback, "", 200);
            }
            else if (action.equalsIgnoreCase("cleanup"))
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                logger.info("node tool cleanup being called");
                nodetool.cleanup();
                return buildResponse(callback, "", 200);
            }
            else if (action.equalsIgnoreCase("repair"))
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                logger.info("node tool repair is being called");
                nodetool.repair();
                return buildResponse(callback, "", 200);
            }
            else if (action.equalsIgnoreCase("refresh"))
            {
                logger.info("node tool refresh is being called");
                if (value == null)
                {
                    logger.error("No value specified with refresh");
                    return buildResponse(callback, "", 404);
                }
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.refresh(Lists.newArrayList(value.split(",")));
                return buildResponse(callback, "", 200);
            }
            else
                return buildResponse(callback, "", 404);
        }
        catch (Exception e)
        {
            return Response.serverError().build();
        }
        return buildResponse(callback, "", 200);
    }

    private Response buildResponse(String callback, String payload, int errorcode)
    {
        if (errorcode != 200)
        {
            if (callback != null)
                return Response.status(errorcode).header("Content-Type", "application/javascript").build();
            else
                return Response.status(errorcode).build();

        }
        else
        {
            if (callback != null)
                return Response.ok(callback + "(" + payload + ")").header("Content-Type", "application/javascript").build();
            else
                return Response.ok(payload).build();
        }
    }
}
