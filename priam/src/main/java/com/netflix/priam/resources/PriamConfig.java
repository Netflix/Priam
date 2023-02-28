/*
 * Copyright 2018 Netflix, Inc.
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

import com.netflix.priam.PriamServer;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This servlet will provide the configuration API service for use by external scripts and tooling
 */
@Path("/v1/config")
@Produces(MediaType.APPLICATION_JSON)
public class PriamConfig {
    private static final Logger logger = LoggerFactory.getLogger(PriamConfig.class);
    private final PriamServer priamServer;

    @Inject
    public PriamConfig(PriamServer server) {
        this.priamServer = server;
    }

    private Response doGetPriamConfig(String group, String name) {
        try {
            final Map<String, Object> result = new HashMap<>();
            final Map<String, Object> value =
                    priamServer.getConfiguration().getStructuredConfiguration(group);
            if (name != null && value.containsKey(name)) {
                result.put(name, value.get(name));
                return Response.ok(GsonJsonSerializer.getGson().toJson(result)).build();
            } else if (name != null) {
                result.put("message", String.format("No such structured config: [%s]", name));
                logger.error(String.format("No such structured config: [%s]", name));
                return Response.status(404)
                        .entity(GsonJsonSerializer.getGson().toJson(result))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            } else {
                result.putAll(value);
                return Response.ok(GsonJsonSerializer.getGson().toJson(result)).build();
            }
        } catch (Exception e) {
            logger.error("Error while executing getPriamConfig", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/structured/{group}")
    public Response getPriamConfig(@PathParam("group") String group) {
        return doGetPriamConfig(group, null);
    }

    @GET
    @Path("/structured/{group}/{name}")
    public Response getPriamConfigByName(
            @PathParam("group") String group, @PathParam("name") String name) {
        return doGetPriamConfig(group, name);
    }

    @GET
    @Path("/unstructured/{name}")
    public Response getProperty(
            @PathParam("name") String name, @QueryParam("default") String defaultValue) {
        Map<String, Object> result = new HashMap<>();
        try {
            String value = priamServer.getConfiguration().getProperty(name, defaultValue);
            if (value != null) {
                result.put(name, value);
                return Response.ok(GsonJsonSerializer.getGson().toJson(result)).build();
            } else {
                result.put("message", String.format("No such property: [%s]", name));
                logger.error(String.format("No such property: [%s]", name));
                return Response.status(404)
                        .entity(GsonJsonSerializer.getGson().toJson(result))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
        } catch (Exception e) {
            logger.error("Error while executing getPriamConfig", e);
            return Response.serverError().build();
        }
    }
}
