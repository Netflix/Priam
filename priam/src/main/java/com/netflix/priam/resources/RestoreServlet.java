/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.resources;

import com.netflix.priam.health.InstanceState;
import com.netflix.priam.restore.Restore;
import com.netflix.priam.utils.DateUtil;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
public class RestoreServlet {

    private static final Logger logger = LoggerFactory.getLogger(RestoreServlet.class);
    private final Restore restoreObj;
    private final InstanceState instanceState;

    @Inject
    public RestoreServlet(Restore restoreObj, InstanceState instanceState) {
        this.restoreObj = restoreObj;
        this.instanceState = instanceState;
    }

    /*
     * @return metadata of current restore.  If no restore in progress, returns the metadata of most recent restore attempt.
     * restoreStatus: {
     * startDateRange: "[yyyymmddhhmm]",
     * endDateRange: "[yyyymmddhhmm]",
     * executionStartTime: "[yyyymmddhhmm]",
     * executionEndTime: "[yyyymmddhhmm]",
     * snapshotMetaFile: "<meta.json> used for full snapshot",
     * status: "STARTED|FINISHED|FAILED"
     * }
     */
    @GET
    @Path("/restore/status")
    public Response status() throws Exception {
        return Response.ok(instanceState.getRestoreStatus().toString()).build();
    }

    @GET
    @Path("/restore")
    public Response restore(@QueryParam("daterange") String daterange) throws Exception {
        DateUtil.DateRange dateRange = new DateUtil.DateRange(daterange);
        logger.info(
                "Parameters: {startTime: [{}], endTime: [{}]}",
                dateRange.getStartTime().toString(),
                dateRange.getEndTime().toString());
        restoreObj.restore(dateRange);
        return Response.ok("[\"ok\"]", MediaType.APPLICATION_JSON).build();
    }
}
