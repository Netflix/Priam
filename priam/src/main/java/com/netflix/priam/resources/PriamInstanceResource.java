/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.resources;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import java.net.URI;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource for manipulating priam instances. */
@Path("/v1/instances")
@Produces(MediaType.TEXT_PLAIN)
public class PriamInstanceResource {
    private static final Logger log = LoggerFactory.getLogger(PriamInstanceResource.class);

    private final IConfiguration config;
    private final IPriamInstanceFactory factory;
    private final InstanceInfo instanceInfo;

    @Inject // Note: do not parameterize the generic type variable to an implementation as it
    // confuses
    // Guice in the binding.
    public PriamInstanceResource(
            IConfiguration config, IPriamInstanceFactory factory, InstanceInfo instanceInfo) {
        this.config = config;
        this.factory = factory;
        this.instanceInfo = instanceInfo;
    }

    /**
     * Get the list of all priam instances
     *
     * @return the list of all priam instances
     */
    @GET
    public String getInstances() {
        return factory.getAllIds(config.getAppName())
                .stream()
                .map(PriamInstance::toString)
                .collect(Collectors.joining("\n", "", "\n"));
    }

    /**
     * Returns an individual priam instance by id or WebApplicationException (404) if not found
     *
     * @param id the node id
     * @return the priam instance
     */
    @GET
    @Path("{id}")
    public String getInstance(@PathParam("id") int id) {
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
            @QueryParam("id") int id,
            @QueryParam("instanceID") String instanceID,
            @QueryParam("hostname") String hostname,
            @QueryParam("ip") String ip,
            @QueryParam("rack") String rack,
            @QueryParam("token") String token) {
        log.info(
                "Creating instance [id={}, instanceId={}, hostname={}, ip={}, rack={}, token={}",
                id,
                instanceID,
                hostname,
                ip,
                rack,
                token);
        PriamInstance instance =
                factory.create(
                        config.getAppName(), id, instanceID, hostname, ip, rack, null, token);
        URI uri = UriBuilder.fromPath("/{id}").build(instance.getId());
        return Response.created(uri).build();
    }

    /**
     * Deletes the instance with the given {@code id}.
     *
     * @param id the node id
     * @return Response (204) if the instance was deleted
     */
    @DELETE
    @Path("{id}")
    public Response deleteInstance(@PathParam("id") int id) {
        PriamInstance instance = getByIdIfFound(id);
        factory.delete(instance);
        return Response.noContent().build();
    }

    /**
     * Returns the PriamInstance with the given {@code id}, or throws a WebApplicationException(400)
     * if none found.
     *
     * @param id the node id
     * @return PriamInstance with the given {@code id}
     */
    private PriamInstance getByIdIfFound(int id) {
        PriamInstance instance =
                factory.getInstance(config.getAppName(), instanceInfo.getRegion(), id);
        if (instance == null) {
            throw notFound(String.format("No priam instance with id %s found", id));
        }
        return instance;
    }

    private static WebApplicationException notFound(String message) {
        return new WebApplicationException(
                Response.status(Response.Status.NOT_FOUND).entity(message).build());
    }
}
