/*
 * Copyright 2017 Netflix, Inc.
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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cluster.management.Compaction;
import com.netflix.priam.cluster.management.Flush;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.utils.JMXConnectionException;
import com.netflix.priam.utils.JMXNodeTool;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Do general operations. Start/Stop and some JMX node tool commands
 */
@SuppressWarnings("deprecation")
@Path("/v1/cassadmin")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraAdmin {
    private static final String REST_HEADER_KEYSPACES = "keyspaces";
    private static final String REST_HEADER_CFS = "cfnames";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final Logger logger = LoggerFactory.getLogger(CassandraAdmin.class);
    private final IConfiguration config;
    private final ICassandraProcess cassProcess;
    private final Flush flush;
    private final Compaction compaction;

    @Inject
    public CassandraAdmin(IConfiguration config, ICassandraProcess cassProcess, Flush flush, Compaction compaction) {
        this.config = config;
        this.cassProcess = cassProcess;
        this.flush = flush;
        this.compaction = compaction;
    }

    @GET
    @Path("/start")
    public Response cassStart() throws IOException, InterruptedException, JSONException {
        cassProcess.start(true);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/stop")
    public Response cassStop(@DefaultValue("false") @QueryParam("force") boolean force) throws IOException, InterruptedException, JSONException {
        cassProcess.stop(force);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/refresh")
    public Response cassRefresh(@QueryParam(REST_HEADER_KEYSPACES) String keyspaces) throws IOException, ExecutionException, InterruptedException, JSONException {
        logger.debug("node tool refresh is being called");
        if (StringUtils.isBlank(keyspaces))
            return Response.status(400).entity("Missing keyspace in request").build();

        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        nodeTool.refresh(Lists.newArrayList(keyspaces.split(",")));
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/info")
    public Response cassInfo() throws IOException, InterruptedException, JSONException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        logger.debug("node tool info being called");
        return Response.ok(nodeTool.info(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/partitioner")
    public Response cassPartitioner() throws IOException, InterruptedException, JSONException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        logger.debug("node tool getPartitioner being called");
        return Response.ok(nodeTool.getPartitioner(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/ring/{id}")
    public Response cassRing(@PathParam("id") String keyspace) throws IOException, InterruptedException, JSONException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        logger.debug("node tool ring being called");
        return Response.ok(nodeTool.ring(keyspace), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/flush")
    public Response cassFlush() throws IOException, InterruptedException, ExecutionException {
        JSONObject rootObj = new JSONObject();

        try {
            flush.execute();
            rootObj.put("Flushed", true);
            return Response.ok().entity(rootObj).build();
        } catch (Exception e) {
            try {
                rootObj.put("status", "ERRROR");
                rootObj.put("desc", e.getLocalizedMessage());
            } catch (Exception e1) {
                return Response.status(503).entity("FlushError")
                        .build();
            }
            return Response.status(503).entity(rootObj)
                    .build();
        }
    }

    @GET
    @Path("/compact")
    public Response cassCompact() throws IOException, ExecutionException, InterruptedException {
        JSONObject rootObj = new JSONObject();

        try {
            compaction.execute();
            rootObj.put("Compcated", true);
            return Response.ok().entity(rootObj).build();
        } catch (Exception e) {
            try {
                rootObj.put("status", "ERRROR");
                rootObj.put("desc", e.getLocalizedMessage());
            } catch (Exception e1) {
                return Response.status(503).entity("CompactionError")
                        .build();
            }
            return Response.status(503).entity(rootObj)
                    .build();
        }
    }

    @GET
    @Path("/cleanup")
    public Response cassCleanup() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        logger.debug("node tool cleanup being called");
        nodeTool.cleanup();
        return Response.ok().build();
    }

    @GET
    @Path("/repair")
    public Response cassRepair(@QueryParam("sequential") boolean isSequential, @QueryParam("localDC") boolean localDCOnly, @DefaultValue("false") @QueryParam("primaryRange") boolean primaryRange) throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        logger.debug("node tool repair being called");
        nodeTool.repair(isSequential, localDCOnly, primaryRange);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/version")
    public Response version() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        return Response.ok(new JSONArray().put(nodeTool.getReleaseVersion()), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/disablegossip")
    public Response disablegossip() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        nodeTool.stopGossiping();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/enablegossip")
    public Response enablegossip() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        nodeTool.startGossiping();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/gossipinfo")
    public Response gossipinfo() throws IOException, ExecutionException, InterruptedException, JSONException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        JSONObject rootObj = parseGossipInfo(nodeTool.getGossipInfo(false));
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }


    // helper method for parsing, to be tested easily
    private static JSONObject parseGossipInfo(String gossipinfo) throws JSONException {
        String[] ginfo = gossipinfo.split("\n");
        JSONObject rootObj = new JSONObject();
        JSONObject obj = new JSONObject();
        String key = "";
        for (String line : ginfo) {
            if (line.matches("^.*/.*$")) {
                String[] data = line.split("/");
                if (StringUtils.isNotBlank(key)) {
                    rootObj.put(key, obj);
                    obj = new JSONObject();
                }
                key = data[1];
            } else if (line.matches("^  .*:.*$")) {
                String[] kv = line.split(":");
                kv[0] = kv[0].trim();
                if (kv[0].equals("STATUS")) {
                    obj.put(kv[0], kv[1]);
                    String[] vv = kv[1].split(",");
                    obj.put("Token", vv[1]);
                } else {
                    obj.put(kv[0], kv[1]);
                }
            }
        }
        if (StringUtils.isNotBlank(key))
            rootObj.put(key, obj);
        return rootObj;
    }


    @GET
    @Path("/move")
    public Response moveToken(@QueryParam(REST_HEADER_TOKEN) String newToken) throws IOException, ExecutionException, InterruptedException, ConfigurationException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        nodeTool.move(newToken);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }



    @GET
    @Path("/drain")
    public Response cassDrain() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodeTool;
        try {
            nodeTool = JMXNodeTool.instance(config);
        } catch (JMXConnectionException e) {
            logger.error("Exception in fetching c* jmx tool .  Msgl: {}", e.getLocalizedMessage(), e);
            return Response.status(503).entity("JMXConnectionException")
                    .build();
        }
        logger.debug("node tool drain being called");
        nodeTool.drain();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    /*
    @parm in - absolute path on disk of compressed file.
    @param out - absolute path on disk for output, decompressed file
    @parapm compression algorithn -- optional and if not provided, defaults to Snappy
    */
    @GET
    @Path("/decompress")
    public Response decompress(@QueryParam("in") String in, @QueryParam("out") String out) throws Exception {
        SnappyCompression compress = new SnappyCompression();
        compress.decompressAndClose(new FileInputStream(in), new FileOutputStream(out));
        JSONObject object = new JSONObject();
        object.put("Input compressed file", in);
        object.put("Output decompress file", out);
        return Response.ok(object.toString(), MediaType.APPLICATION_JSON).build();
    }

}
