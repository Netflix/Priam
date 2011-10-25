package com.priam.servlets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.priam.conf.IConfiguration;
import com.priam.conf.JMXNodeTool;
import com.priam.conf.PriamServer;
import com.priam.utils.SystemUtils;

/**
 * Do general operartion. For now start/stop 
 */
@Path("/cassandra_admin")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraAdmin
{

    public static final String REST_HEADER_ACTION = "action";
    public static final String REST_HEADER_NODETOOL = "nodetool";
    private static final Logger logger = LoggerFactory.getLogger(CassandraAdmin.class);

    //@Inject
    //IConfiguration config;
    
    @GET
    //@Produces(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response runAction(@QueryParam(REST_HEADER_ACTION) String action)
    {
        try
        {
        	//JMXNodeTool nodetool = JMXNodeTool.instance(config);
            JMXNodeTool nodetool = JMXNodeTool.instance(PriamServer.instance.config);

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
            	logger.info("node tool info being called");
                return Response.ok(nodetool.info().toJSONString()).build();
            } 
            else if (action.equalsIgnoreCase("ring"))
            {
            	logger.info("node tool ring being called");
                return Response.ok(nodetool.ring().toString()).build();
            } 
            else if (action.equalsIgnoreCase("flush")){
            	logger.info("node tool flush being called");
            	nodetool.flush();
            	return Response.status(200).build();
            }
            else if (action.equalsIgnoreCase("repair")){
            	logger.info("node tool repair is being called");
            	nodetool.repair();
            	return Response.status(200).build();
            }
            else
                return Response.status(404).build();
        } catch (Exception e)
        {
            return Response.serverError().build();
        }
        return Response.ok().build();
    }
}
