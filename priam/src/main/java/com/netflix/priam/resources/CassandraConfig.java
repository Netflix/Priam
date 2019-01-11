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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.PriamServer;
import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.merics.Metrics;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import org.json.simple.JSONValue;

/**
 * This servlet will provide the configuration API service as and when Cassandra requests for it.
 */
@Path("/v1/cassconfig")
@Produces(MediaType.TEXT_PLAIN)
public class CassandraConfig {
    private static final Logger logger = LoggerFactory.getLogger(CassandraConfig.class);
    private final PriamServer priamServer;
    private final DoubleRing doubleRing;
    private final AtomicLong getSeedsCnt, getTokenCnt, getReplacedIpCnt, doubleRingCnt;

    @Inject
    public CassandraConfig(PriamServer server, DoubleRing doubleRing, Registry registry) {
        this.priamServer = server;
        this.doubleRing = doubleRing;

        getSeedsCnt = PolledMeter.using(registry)
                                 .withName(Metrics.METRIC_PREFIX + "cass.getSeedCnt")
                                 .monitorMonotonicCounter(new AtomicLong());
        getTokenCnt = PolledMeter.using(registry)
                                 .withName(Metrics.METRIC_PREFIX + "cass.getTokenCnt")
                                 .monitorMonotonicCounter(new AtomicLong());
        getReplacedIpCnt = PolledMeter.using(registry)
                                      .withName(Metrics.METRIC_PREFIX + "cass.getReplacedIpCnt")
                                      .monitorMonotonicCounter(new AtomicLong());

        doubleRingCnt = PolledMeter.using(registry)
                                   .withName(Metrics.METRIC_PREFIX + "cass.doubleRingCnt")
                                   .monitorMonotonicCounter(new AtomicLong());


    }

    @GET
    @Path("/get_seeds")
    public Response getSeeds() {
        try {
            final List<String> seeds = priamServer.getInstanceIdentity().getSeeds();
            if (!seeds.isEmpty())
            {
                getSeedsCnt.incrementAndGet();
                return Response.ok(StringUtils.join(seeds, ',')).build();
            }
            logger.error("Cannot find the Seeds");
        } catch (Exception e) {
            logger.error("Error while executing get_seeds", e);
            return Response.serverError().build();
        }
        return Response.status(500).build();
    }

    @GET
    @Path("/get_token")
    public Response getToken() {
        try {
            String token = priamServer.getInstanceIdentity().getInstance().getToken();
            if (StringUtils.isNotBlank(token)) {
                logger.info("Returning token value \"{}\" for this instance to caller.", token);
                getTokenCnt.incrementAndGet();
                return Response.ok(priamServer.getInstanceIdentity().getInstance().getToken())
                               .build();
            }

            logger.error("Cannot find token for this instance.");
        } catch (Exception e) {
            // TODO: can this ever happen? if so, what conditions would cause an exception here?
            logger.error("Error while executing get_token", e);
            return Response.serverError().build();
        }
        return Response.status(500).build();
    }

    @GET
    @Path("/is_replace_token")
    public Response isReplaceToken() {
        try {
            return Response.ok(String.valueOf(priamServer.getInstanceIdentity().isReplace()))
                           .build();
        } catch (Exception e) {
            // TODO: can this ever happen? if so, what conditions would cause an exception here?
            logger.error("Error while executing is_replace_token", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/get_replaced_ip")
    public Response getReplacedIp() {
        try {
            getReplacedIpCnt.incrementAndGet();
            return Response.ok(String.valueOf(priamServer.getInstanceIdentity().getReplacedIp()))
                           .build();
        } catch (Exception e) {
            logger.error("Error while executing get_replaced_ip", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/get_extra_env_params")
    public Response getExtraEnvParams() {
        try {
            Map<String, String> returnMap;
            returnMap = priamServer.getConfiguration().getExtraEnvParams();
            if (returnMap == null) {
                returnMap = new HashMap<>();
            }
            String extraEnvParamsJson = JSONValue.toJSONString(returnMap);
            return Response.ok(extraEnvParamsJson).build();
        } catch (Exception e) {
            logger.error("Error while executing get_extra_env_params", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/double_ring")
    public Response doubleRing() throws IOException, ClassNotFoundException {
        try {
            doubleRingCnt.incrementAndGet();
            doubleRing.backup();
            doubleRing.doubleSlots();
        } catch (Throwable th) {
            logger.error("Error in doubling the ring...", th);
            doubleRing.restore();
            // rethrow
            throw new RuntimeException(th);
        }
        return Response.status(200).build();
    }
}

