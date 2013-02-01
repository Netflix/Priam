/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.resources;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.PriamServer;

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

    @Inject
    public CassandraConfig(PriamServer server)
    {
        this.priamServer = server;
    }

    @GET
    @Path("/get_seeds")
    public Response getSeeds()
    {
        try
        {
            final List<String> seeds = priamServer.getId().getSeeds();
            logger.info("seeds to return:  " + StringUtils.join(seeds, ','));
            if (!seeds.isEmpty())
                return Response.ok(StringUtils.join(seeds, ',')).build();
            logger.error("Cannot find the Seeds");
        }
        catch (Exception e)
        {
            logger.error("Error while executing get_seeds", e);
            return Response.serverError().build();
        }
        return Response.status(500).build();
    }

    @GET
    @Path("/get_token")
    public Response getToken()
    {
//        try
//        {
//            if (StringUtils.isNotBlank(priamServer.getId().getInstance().getToken()))
//                return Response.ok(priamServer.getId().getInstance().getToken()).build();
//            logger.error("Cannot find token for this instance.");
//        }
//        catch (Exception e)
//        {
//            // TODO: can this ever happen? if so, what conditions would cause an exception here?
//            logger.error("Error while executing get_token", e);
//            return Response.serverError().build();
//        }
        //TODO: punting on tokens for now
        return Response.status(200).build();
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
}
