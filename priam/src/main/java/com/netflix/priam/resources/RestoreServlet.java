/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.resources;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.restore.Restore;
import com.netflix.priam.restore.RestoreTokenSelector;
import com.netflix.priam.tuner.ICassandraTuner;
import com.netflix.priam.utils.ITokenManager;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;

@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
public class RestoreServlet {

    private static final Logger logger = LoggerFactory.getLogger(RestoreServlet.class);
    private static final String REST_HEADER_RANGE = "daterange";
    private static final String REST_HEADER_REGION = "region";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_KEYSPACES = "keyspaces";
    private static final String REST_RESTORE_PREFIX = "restoreprefix";
    private static final String REST_SUCCESS = "[\"ok\"]";

    private IConfiguration config;
    private Restore restoreObj;
    private Provider<AbstractBackupPath> pathProvider;
    private PriamServer priamServer;
    private ICassandraTuner tuner;
    private ICassandraProcess cassProcess;
    private RestoreTokenSelector tokenSelector;
    private InstanceState instanceState;

    @Inject
    public RestoreServlet(IConfiguration config, Restore restoreObj, Provider<AbstractBackupPath> pathProvider, PriamServer priamServer
            , RestoreTokenSelector tokenSelector, ICassandraTuner tuner, ICassandraProcess cassProcess, InstanceState instanceState) {
        this.config = config;
        this.restoreObj = restoreObj;
        this.pathProvider = pathProvider;
        this.priamServer = priamServer;
        this.tuner = tuner;
        this.cassProcess = cassProcess;
        this.tokenSelector = tokenSelector;
        this.instanceState = instanceState;
    }


    /*
     * @return metadata of current restore.  If no restore in progress, returns the metadata of most recent restore attempt.
     * status:[not_started|running|success|failure]
     * daterange:[startdaterange,enddatarange]
     * starttime:[yyyymmddhhmm]
     * endtime:[yyyymmddmm]
     */
    @GET
    @Path("/restore/status")
    public Response status() throws Exception {
        return Response.ok(instanceState.getRestoreStatus().toString()).build();
    }

    @GET
    @Path("/restore")
    public Response restore(@QueryParam(REST_HEADER_RANGE) String daterange, @QueryParam(REST_HEADER_REGION) String region, @QueryParam(REST_HEADER_TOKEN) String token,
                            @QueryParam(REST_KEYSPACES) String keyspaces, @QueryParam(REST_RESTORE_PREFIX) String restorePrefix) throws Exception {
        Date startTime;
        Date endTime;

        if (StringUtils.isBlank(daterange) || daterange.equalsIgnoreCase("default")) {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        } else {
            String[] restore = daterange.split(",");
            AbstractBackupPath path = pathProvider.get();
            startTime = path.parseDate(restore[0]);
            endTime = path.parseDate(restore[1]);
        }

        String origRestorePrefix = config.getRestorePrefix();
        if (StringUtils.isNotBlank(restorePrefix)) {
            config.setRestorePrefix(restorePrefix);
        }

        logger.info("Parameters: { token: [{}], region: [{}], startTime: [{}], endTime: [{}], keyspaces: [{}], restorePrefix: [{}]}",
                token, region, startTime, endTime, keyspaces, restorePrefix);

        restore(token, region, startTime, endTime, keyspaces);

        //Since this call is probably never called in parallel, config is multi-thread safe to be edited
        if (origRestorePrefix != null)
            config.setRestorePrefix(origRestorePrefix);
        else config.setRestorePrefix("");

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    /**
     * Restore with the specified start and end time.
     *
     * @param token     Overrides the current token with this one, if specified
     * @param region    Override the region for searching backup
     * @param startTime Start time
     * @param endTime   End time upto which the restore should fetch data
     * @param keyspaces Comma seperated list of keyspaces to restore
     * @throws Exception
     */
    private void restore(String token, String region, Date startTime, Date endTime, String keyspaces) throws Exception {
        String origRegion = config.getDC();
        String origBackupIdentifier = priamServer.getId().getBackupIdentifier();
        if (StringUtils.isNotBlank(token))
        {
            priamServer.getId().setBackupIdentifier(token);
        }
        else {
            token = priamServer.getId().getToken();
        }

        if (config.isRestoreClosestToken())
            priamServer.getId().setBackupIdentifier(tokenSelector.getClosestToken(token, config.getAppName(), config.getDC()).toString());

        if (StringUtils.isNotBlank(region)) {
            config.setDC(region);
            logger.info("Restoring from region {}", region);
            priamServer.getId().setBackupIdentifier(tokenSelector.getClosestToken(token, config.getAppName(), region).toString());
            logger.info("Restore will use backup identifier {}", priamServer.getId().getBackupIdentifier());
        }

        setRestoreKeyspaces(keyspaces);

        try {
            restoreObj.restore(startTime, endTime);
        } finally {
            config.setDC(origRegion);
            priamServer.getId().setBackupIdentifier(origBackupIdentifier);
        }
        tuner.updateAutoBootstrap(config.getYamlLocation(), false);
        cassProcess.start(true);
    }

    /*
     * TODO: decouple the servlet, config, and restorer. this should not rely on a side
     *       effect of a list mutation on the config object (treating it as global var).
     */
    private void setRestoreKeyspaces(String keyspaces) {
        if (StringUtils.isNotBlank(keyspaces)) {
            List<String> newKeyspaces = Lists.newArrayList(keyspaces.split(","));
            config.setRestoreKeySpaces(newKeyspaces);
        }
    }

}