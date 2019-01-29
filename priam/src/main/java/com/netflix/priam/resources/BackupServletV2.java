/*
 * Copyright 2019 Netflix, Inc.
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

import com.google.inject.Inject;
import com.netflix.priam.backup.BackupMetadata;
import com.netflix.priam.backup.BackupVerification;
import com.netflix.priam.backup.BackupVerificationResult;
import com.netflix.priam.backup.BackupVersion;
import com.netflix.priam.backup.IBackupStatusMgr;
import com.netflix.priam.services.BackupTTLService;
import com.netflix.priam.services.SnapshotMetaService;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 1/16/19. */
@Path("/v2/backup")
@Produces(MediaType.APPLICATION_JSON)
public class BackupServletV2 {
    private static final Logger logger = LoggerFactory.getLogger(BackupServletV2.class);
    private final BackupVerification backupVerification;
    private final IBackupStatusMgr backupStatusMgr;
    private final SnapshotMetaService snapshotMetaService;
    private final BackupTTLService backupTTLService;
    private static final String REST_SUCCESS = "[\"ok\"]";

    @Inject
    public BackupServletV2(
            IBackupStatusMgr backupStatusMgr,
            BackupVerification backupVerification,
            SnapshotMetaService snapshotMetaService,
            BackupTTLService backupTTLService) {
        this.backupStatusMgr = backupStatusMgr;
        this.backupVerification = backupVerification;
        this.snapshotMetaService = snapshotMetaService;
        this.backupTTLService = backupTTLService;
    }

    @GET
    @Path("/do_snapshot")
    public Response backup() throws Exception {
        snapshotMetaService.execute();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/ttl")
    public Response ttl() throws Exception {
        backupTTLService.execute();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/info/{date}")
    public Response info(@PathParam("date") String date) {
        Instant instant = DateUtil.parseInstant(date);
        List<BackupMetadata> metadataList =
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        new DateRange(
                                instant,
                                instant.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS)));
        return Response.ok(metadataList).build();
    }

    @GET
    @Path("/validate/{daterange}")
    public Response validateV2SnapshotByDate(
            @PathParam("daterange") String daterange,
            @DefaultValue("false") @QueryParam("force") boolean force)
            throws Exception {
        DateUtil.DateRange dateRange = new DateUtil.DateRange(daterange);
        Optional<BackupVerificationResult> result =
                backupVerification.verifyBackup(
                        BackupVersion.SNAPSHOT_META_SERVICE, force, dateRange);
        if (!result.isPresent()) {
            return Response.noContent()
                    .entity("No valid meta found for provided time range")
                    .build();
        }

        return Response.ok(result.get()).build();
    }
}
