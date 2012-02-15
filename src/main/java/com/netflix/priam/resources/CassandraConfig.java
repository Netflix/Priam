package com.netflix.priam.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.PriamServer;
import com.netflix.priam.identity.DoubleRing;

/**
 * This servlet will provide the configuration API service as and when Cassandra
 * requests for it.
 */
@Path("/v1/cassconfig/{action}")
@Produces({ "application/text" })
public class CassandraConfig
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraConfig.class);
    private static final String REST_HEADER_TYPE = "action";
    
    private PriamServer priamServer;
    private DoubleRing doubleRing;
    
    @Inject
    public CassandraConfig(PriamServer server, DoubleRing doubleRing){
        this.priamServer = server;
        this.doubleRing = doubleRing;
    }

    
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getConfigGET(@PathParam(REST_HEADER_TYPE) String action)
    {
        logger.info("Trying to run command: " + action);
        return getTypedResponse(action);
    }

    private Response getTypedResponse(String action)
    {
        try {
            if (action.equalsIgnoreCase("GET_SEEDS")) {
                if (priamServer.getId().getSeeds() != null && priamServer.getId().getSeeds().size() != 0)
                    return Response.ok(StringUtils.join(priamServer.getId().getSeeds(), ',')).build();
                logger.error("Cannot find the Seeds " + priamServer.getId().getSeeds());
            }
            else if (action.equalsIgnoreCase("GET_TOKEN")) {
                if (priamServer.getId().getInstance().getPayload() != null)
                    return Response.ok(priamServer.getId().getInstance().getPayload()).build();
                logger.error("Cannot find the token...:" + priamServer.getId().getInstance().getPayload());
            }
            else if (action.equalsIgnoreCase("IS_REPLACE_TOKEN")) {
                return Response.ok("" + priamServer.getId().isReplace()).build();
            }
            else if (action.equalsIgnoreCase("DOUBLE_RING")) {                
                try {
                    doubleRing.backup();
                    doubleRing.doubleSlots();
                }
                catch (Throwable th) {
                    logger.error("Error in doubling the ring...", th);
                    doubleRing.restore();
                    // rethrow
                    throw new RuntimeException(th);
                }
                return Response.status(200).build();
            }
        }
        catch (Exception e) {
            logger.error("Error while executing the servlet... : ", e);
        }
        logger.error(String.format("Couldnt serve the URL with parameters: type=%s", action));
        return Response.status(404).build();
    }
}
