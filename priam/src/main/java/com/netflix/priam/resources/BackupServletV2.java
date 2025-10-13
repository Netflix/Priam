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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.*;
import com.netflix.priam.backupv2.BackupTTLTask;
import com.netflix.priam.backupv2.BackupV2Service;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.backupv2.SnapshotMetaTask;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 1/16/19. */
@Path("/v2/backup")
@Produces(MediaType.APPLICATION_JSON)
public class BackupServletV2 {
    private static final Logger logger = LoggerFactory.getLogger(BackupServletV2.class);
    private static final String INCREMENTALS = "/" + AbstractBackup.INCREMENTAL_BACKUP_FOLDER + "/";
    private static final String SNAPSHOTS = "/" + SnapshotMetaTask.SNAPSHOT_PREFIX;
    private final BackupVerification backupVerification;
    private final IBackupStatusMgr backupStatusMgr;
    private final SnapshotMetaTask snapshotMetaService;
    private final BackupTTLTask backupTTLService;
    private final IBackupFileSystem fs;
    private final IMetaProxy metaProxy;
    private final Provider<AbstractBackupPath> pathProvider;
    private final BackupV2Service backupService;
    private final BackupNotificationMgr backupNotificationMgr;
    private final IConfiguration config;
    private final DirectorySize directorySize;

    private static final String REST_SUCCESS = "[\"ok\"]";

    @Inject
    public BackupServletV2(
            IBackupStatusMgr backupStatusMgr,
            BackupVerification backupVerification,
            SnapshotMetaTask snapshotMetaService,
            BackupTTLTask backupTTLService,
            IBackupFileSystem fileSystem,
            @Named("v2") IMetaProxy metaV2Proxy,
            Provider<AbstractBackupPath> pathProvider,
            BackupV2Service backupService,
            BackupNotificationMgr backupNotificationMgr,
            IConfiguration config,
            DirectorySize directorySize) {
        this.backupStatusMgr = backupStatusMgr;
        this.backupVerification = backupVerification;
        this.snapshotMetaService = snapshotMetaService;
        this.backupTTLService = backupTTLService;
        this.fs = fileSystem;
        this.metaProxy = metaV2Proxy;
        this.pathProvider = pathProvider;
        this.backupService = backupService;
        this.backupNotificationMgr = backupNotificationMgr;
        this.config = config;
        this.directorySize = directorySize;
    }

    @GET
    @Path("/do_snapshot")
    public Response backup() throws Exception {
        try {
            snapshotMetaService.execute();
        } catch (Exception e) {
            if (e instanceof IllegalStateException) {
                return Response.status(409).build();
            }
            return Response.serverError().build();
        }
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/ttl")
    public Response ttl() throws Exception {
        backupTTLService.execute();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/clearCache")
    public Response clearCache() throws Exception {
        fs.clearCache();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/updateService")
    public Response updateService() throws Exception {
        backupService.onChangeUpdateService();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/info/{date}")
    public Response info(@PathParam("date") String date) {
        Instant instant = DateUtil.parseInstant(date);
        List<BackupMetadata> metadataList =
                backupStatusMgr.getLatestBackupMetadata(
                        new DateRange(
                                instant,
                                instant.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS)));
        return Response.ok(GsonJsonSerializer.getGson().toJson(metadataList)).build();
    }

    @GET
    @Path("/validate/{daterange}")
    public Response validateV2SnapshotByDate(
            @PathParam("daterange") String daterange,
            @DefaultValue("false") @QueryParam("force") boolean force)
            throws Exception {
        DateUtil.DateRange dateRange = new DateUtil.DateRange(daterange);
        Optional<BackupVerificationResult> result =
                backupVerification.verifyLatestBackup(force, dateRange);
        if (!result.isPresent()) {
            return Response.noContent()
                    .entity("No valid meta found for provided time range")
                    .build();
        }

        // Send notification for any verified backups. This is useful in one-off backup consumption
        // by downward dependencies.
        // Side-effect: It may send notification for already verified snapshot i.e. duplicate
        // message may be sent.
        logger.info(
                "Sending {} message for backup: {}",
                AbstractBackupPath.BackupFileType.SNAPSHOT_VERIFIED,
                result.get().remotePath);

        backupNotificationMgr.notify(result.get().remotePath, result.get().snapshotInstant);

        return Response.ok(result.get().toString()).build();
    }

    @GET
    @Path("/list/{daterange}")
    public Response list(@PathParam("daterange") String daterange) throws Exception {
        DateUtil.DateRange dateRange = new DateUtil.DateRange(daterange);
        Optional<AbstractBackupPath> metaFile =
                BackupRestoreUtil.getLatestValidMetaPath(metaProxy, dateRange);
        if (!metaFile.isPresent()) {
            return Response.ok("No valid meta found!").build();
        }
        List<AbstractBackupPath> files =
                BackupRestoreUtil.getMostRecentSnapshotPaths(
                        metaFile.get(), metaProxy, pathProvider);
        DateUtil.DateRange incrementalDateRange =
                new DateRange(metaFile.get().getLastModified(), dateRange.getEndTime());
        files.addAll(metaProxy.getIncrementals(incrementalDateRange));
        List<String> remotePaths =
                files.stream().map(AbstractBackupPath::getRemotePath).collect(Collectors.toList());
        return Response.ok(GsonJsonSerializer.getGson().toJson(remotePaths)).build();
    }

    @GET
    @Path("/state/{hours}")
    public Response backupState(@PathParam("hours") int hours) throws Exception {
        Map<String, Object> responseMap = new HashMap<>();

        responseMap.put("tasksQueued", fs.getUploadTasksQueued());
        responseMap.put("queueSize", config.getBackupQueueSize());
        responseMap.put("totalFiles", directorySize.getFiles(config.getDataFileLocation(), SNAPSHOTS, INCREMENTALS));

        List<BackupMetadata> latestBackupMetadata =
                backupStatusMgr.getLatestBackupMetadata(
                        new DateRange(Instant.now().minus(hours, ChronoUnit.HOURS), Instant.now()));
        responseMap.put("latestBackupMetadata", latestBackupMetadata);

        ObjectMapper mapper = new ObjectMapper();
        String jsonResponse = mapper.writeValueAsString(responseMap);
        return Response.ok(jsonResponse).build();
    }
}
