package com.priam.servlets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.priam.conf.JMXNodeTool;
import com.priam.conf.PriamServer;
import com.priam.utils.SystemUtils;

/**
 * Do general operartion. For now start/stop 
 */
@Path("/cassandra_admin")
@Produces({ "application/text" })
public class CassandraAdmin
{

    public static final String REST_HEADER_ACTION = "action";
    public static final String REST_HEADER_NODETOOL = "nodetool";

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response runAction(@QueryParam(REST_HEADER_ACTION) String action)
    {
        try
        {
            if (action.equalsIgnoreCase("start"))
            {
                SystemUtils.startCassandra(true, PriamServer.instance.config);
            } 
            else if (action.equalsIgnoreCase("stop"))
            {
                if (!SystemUtils.stopCassandra())
                    return Response.serverError().build();
            } 
            else if (action.equalsIgnoreCase("info"))
            {
                return Response.ok(PriamServer.instance.injector.getInstance(JMXNodeTool.class).info()).build();
            } 
            else if (action.equalsIgnoreCase("ring"))
            {
                return Response.ok(PriamServer.instance.injector.getInstance(JMXNodeTool.class).info()).build();
            } else
                return Response.status(404).build();
        } catch (Exception e)
        {
            return Response.serverError().build();
        }
        return Response.ok().build();
    }
}
