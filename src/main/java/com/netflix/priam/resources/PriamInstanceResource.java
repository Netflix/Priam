package com.netflix.priam.resources;

import java.net.URI;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource for manipulating priam instances.
 */
@Path("/v1/instances")
@Produces(MediaType.TEXT_PLAIN)
public class PriamInstanceResource
{
    private static final Logger log = LoggerFactory.getLogger(PriamInstanceResource.class);

    private final IConfiguration config;
    private final IPriamInstanceFactory factory;

    @Inject
    public PriamInstanceResource(IConfiguration config, IPriamInstanceFactory factory)
    {
        this.config = config;
        this.factory = factory;
    }

    /**
     * Get the list of all priam instances
     * @return the list of all priam instances
     */
    @GET
    public String getInstances()
    {
        StringBuilder response = new StringBuilder();
        for (PriamInstance node : factory.getAllIds(config.getAppName()))
        {
            response.append(node.toString());
            response.append("\n");
        }
        return response.toString();
    }

    /**
     * Returns an individual priam instance by id
     * 
     * @param id the node id
     * @return the priam instance
     * @throws WebApplicationException(404) if no priam instance found with {@code id}
     */
    @GET
    @Path("{id}")
    public String getInstance(@PathParam("id") int id)
    {
        PriamInstance node = getByIdIfFound(id);
        return node.toString();
    }

    /**
     * Creates a new instance with the given parameters
     *
     * @param id the node id
     * @return Response (201) if the instance was created
     */
    @POST
    public Response createInstance(
        @QueryParam("id") int id, @QueryParam("instanceID") String instanceID,
        @QueryParam("hostname") String hostname, @QueryParam("ip") String ip,
        @QueryParam("rack") String rack, @QueryParam("token") String token)
    {
        log.info("Creating instance [id={}, instanceId={}, hostname={}, ip={}, rack={}, token={}",
            new Object[]{ id, instanceID, hostname, ip, rack, token });
        PriamInstance instance = factory.create(config.getAppName(), id, instanceID, hostname, ip, rack, null, token);
        URI uri = UriBuilder.fromPath("/{id}").build(instance.getId());
        return Response.created(uri).build();
    }

    /**
     * Deletes the instance with the given {@code id}.
     * 
     * @param id the node id
     * @return Response (204) if the instance was deleted
     * @throws WebApplicationException (404) if no priam instance found with {@code id}
     */
    @DELETE
    @Path("{id}")
    public Response deleteInstance(@PathParam("id") int id)
    {
        PriamInstance instance = getByIdIfFound(id);
        factory.delete(instance);
        return Response.noContent().build();
    }

    /**
     * Returns the PriamInstance with the given {@code id}, or
     * throws a WebApplicationException if none found.
     * 
     * @param id the node id
     * @return PriamInstance with the given {@code id}
     * @throws WebApplicationException (400)
     */
    private PriamInstance getByIdIfFound(int id)
    {
        PriamInstance instance = factory.getInstance(config.getAppName(), id);
        if (instance == null) {
            throw notFound(String.format("No priam instance with id %s found", id));
        }
        return instance;
    }

    private static WebApplicationException notFound(String message)
    {
        return new WebApplicationException(Response.status(Response.Status.NOT_FOUND).entity(message).build());
    }
}
