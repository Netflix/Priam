package com.netflix.priam.resources;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.collections.CollectionUtils;
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
@Path("/v1/cassconfig")
@Produces(MediaType.TEXT_PLAIN)
public class CassandraConfig
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraConfig.class);
    private PriamServer priamServer;
    private DoubleRing doubleRing;

    @Inject
    public CassandraConfig(PriamServer server, DoubleRing doubleRing)
    {
        this.priamServer = server;
        this.doubleRing = doubleRing;
    }

    @GET
    @Path("/get_seeds")
    public Response getSeeds()
    {
        try
        {
            if (CollectionUtils.isNotEmpty(priamServer.getId().getSeeds()))
                return Response.ok(StringUtils.join(priamServer.getId().getSeeds(), ',')).build();
            logger.error("Cannot find the Seeds " + priamServer.getId().getSeeds());
        }
        catch (Exception e)
        {
            logger.error("Error while executing get_seeds", e);
            return Response.serverError().build();
        }
        return Response.status(404).build();
    }

    @GET
    @Path("/get_token")
    public Response getToken()
    {
        try
        {
            if (StringUtils.isNotBlank(priamServer.getId().getInstance().getToken()))
                return Response.ok(priamServer.getId().getInstance().getToken()).build();
            logger.error("Cannot find token for this instance.");
        }
        catch (Exception e)
        {
            // TODO: can this ever happen? if so, what conditions would cause an exception here?
            logger.error("Error while executing get_token", e);
            return Response.serverError().build();
        }
        return Response.status(404).build();
    }

    @GET
    @Path("/is_replace_token")
    public Response isReplaceToken()
    {
        try
        {
            return Response.ok(String.valueOf(priamServer.getId().isReplace())).build();
        }
        catch (Exception e)
        {
            // TODO: can this ever happen? if so, what conditions would cause an exception here?
            logger.error("Error while executing is_replace_token", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/double_ring")
    public Response doubleRing() throws IOException, ClassNotFoundException
    {
        try
        {
            doubleRing.backup();
            doubleRing.doubleSlots();
        }
        catch (Throwable th)
        {
            logger.error("Error in doubling the ring...", th);
            doubleRing.restore();
            // rethrow
            throw new RuntimeException(th);
        }
        return Response.status(200).build();
    }
}
