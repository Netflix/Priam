package com.priam.servlets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.priam.backup.IBackupFileSystem;
import com.priam.backup.Restore;
import com.priam.backup.SnapshotBackup;
import com.priam.conf.PriamServer;
import com.priam.identity.DoubleRing;

/**
 * This servlet will provide the configuration API service as and when cassandra
 * requests for it.
 * 
 * @author "Vijay Parthasarathy"
 */
@Path("/cassandra_config/{clusterName}")
@Produces({ "application/text" })
public class CConfig
{
    private static final Logger logger = LoggerFactory.getLogger(CConfig.class);
    private static final String REST_HEADER_TYPE = "type";

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getConfigGET(@QueryParam(REST_HEADER_TYPE) String request_type)
    {
        return getTypedResponse(request_type);
    }

    private Response getTypedResponse(String request_type)
    {
        try
        {
            if (request_type.equalsIgnoreCase("GET_SEEDS"))
            {
                if (PriamServer.instance.id.getSeeds() != null && PriamServer.instance.id.getSeeds().size() != 0)
                    return Response.ok(StringUtils.join(PriamServer.instance.id.getSeeds(), ',')).build();
                logger.error("Cannot find the Seeds " + PriamServer.instance.id.getSeeds());
            }
            else if (request_type.equalsIgnoreCase("GET_TOKEN"))
            {
                if (PriamServer.instance.id.getInstance().getPayload() != null)
                    return Response.ok(PriamServer.instance.id.getInstance().getPayload()).build();
                logger.error("Cannot find the token...:" + PriamServer.instance.id.getInstance().getPayload());
            }
            else if (request_type.equalsIgnoreCase("IS_REPLACE_TOKEN"))
            {
                return Response.ok("" + PriamServer.instance.id.isReplace).build();
            }
            else if (request_type.equalsIgnoreCase("DOUBLE_RING"))
            {
                DoubleRing ring = PriamServer.instance.injector.getInstance(DoubleRing.class);
                try
                {
                ring.backup();
                ring.doubleSlots();
                }
                catch (Throwable th)
                {
                    logger.error("Error in doubling the ring...", th);
                    ring.restore();
                    // rethrow
                    throw new RuntimeException(th);
                }
                return Response.status(200).build();
            }
        }
        catch (Exception e)
        {
            logger.error("Error while executing the servlet... : ", e);
        }
        logger.error(String.format("Couldnt serve the URL with parameters: type=%s", request_type));
        return Response.status(404).build();
    }
}
