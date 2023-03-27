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

import com.netflix.priam.backup.*;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/backup")
@Produces(MediaType.APPLICATION_JSON)
public class BackupServlet {
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final String REST_HEADER_RANGE = "daterange";
    private static final String REST_HEADER_FILTER = "filter";
    private final IConfiguration config;
    private final IBackupRestoreConfig backupRestoreConfig;
    private final IBackupFileSystem backupFs;
    private final SnapshotBackup snapshotBackup;
    private final BackupVerification backupVerification;
    @Inject private PriamScheduler scheduler;
    private final IBackupStatusMgr completedBkups;
    private final BackupService backupService;
    @Inject private MetaData metaData;

    @Inject
    public BackupServlet(
            IConfiguration config,
            IBackupRestoreConfig backupRestoreConfig,
            @Named("backup") IBackupFileSystem backupFs,
            SnapshotBackup snapshotBackup,
            IBackupStatusMgr completedBkups,
            BackupVerification backupVerification,
            BackupService backupService) {
        this.config = config;
        this.backupRestoreConfig = backupRestoreConfig;
        this.backupFs = backupFs;
        this.snapshotBackup = snapshotBackup;
        this.completedBkups = completedBkups;
        this.backupVerification = backupVerification;
        this.backupService = backupService;
    }

    @GET
    @Path("/do_snapshot")
    public Response backup() throws Exception {
        snapshotBackup.execute();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/incremental_backup")
    public Response backupIncrementals() throws Exception {
        scheduler.addTask(
                "IncrementalBackup",
                IncrementalBackup.class,
                IncrementalBackup.getTimer(config, backupRestoreConfig));
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/updateService")
    public Response updateService() throws Exception {
        backupService.onChangeUpdateService();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/list")
    /*
     * Fetch the list of files for the requested date range.
     *
     * @param date range
     * @param filter.  The type of data files fetched.  E.g. META will only fetch the daily snapshot meta data file (meta.json).
     * @return the list of files in json format as part of the Http response body.
     */
    public Response list(
            @QueryParam(REST_HEADER_RANGE) String daterange,
            @QueryParam(REST_HEADER_FILTER) @DefaultValue("") String filter)
            throws Exception {

        logger.info(
                "Parameters: {backupPrefix: [{}], daterange: [{}], filter: [{}]}",
                config.getBackupPrefix(),
                daterange,
                filter);

        DateUtil.DateRange dateRange = new DateUtil.DateRange(daterange);

        Iterator<AbstractBackupPath> it =
                backupFs.list(
                        config.getBackupPrefix(),
                        Date.from(dateRange.getStartTime()),
                        Date.from(dateRange.getEndTime()));
        JSONObject object = new JSONObject();
        object = constructJsonResponse(object, it, filter);
        return Response.ok(object.toString(2), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response status() throws Exception {
        JSONObject object = new JSONObject();
        object.put("SnapshotStatus", snapshotBackup.state().toString());
        return Response.ok(object.toString(), MediaType.APPLICATION_JSON).build();
    }

    /*
     * Determines the status of a snapshot for a date.  If there was at least one successful snpashot for the date, snapshot
     * for the date is considered completed.
     * @param date date of the snapshot.  Format of date is yyyymmdd
     * @return {"Snapshotstatus":false} or {"Snapshotstatus":true}
     */
    @GET
    @Path("/status/{date}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response statusByDate(@PathParam("date") String date) throws Exception {
        Instant startTime = DateUtil.parseInstant(date);
        Optional<BackupMetadata> backupMetadataOptional =
                this.completedBkups
                        .getLatestBackupMetadata(
                                BackupVersion.SNAPSHOT_BACKUP,
                                new DateRange(
                                        startTime.truncatedTo(ChronoUnit.DAYS),
                                        startTime
                                                .plus(1, ChronoUnit.DAYS)
                                                .truncatedTo(ChronoUnit.DAYS)))
                        .stream()
                        .findFirst();

        JSONObject object = new JSONObject();
        if (!backupMetadataOptional.isPresent()) {
            object.put("Snapshotstatus", false);
        } else {

            object.put("Snapshotstatus", true);
            object.put("Details", new JSONObject(backupMetadataOptional.get().toString()));
        }
        return Response.ok(object.toString(), MediaType.APPLICATION_JSON).build();
    }

    /*
     * Determines the status of a snapshot for a date.  If there was at least one successful snpashot for the date, snapshot
     * for the date is considered completed.
     * @param date date of the snapshot.  Format of date is yyyymmdd
     * @return {"Snapshots":["201606060450","201606060504"]} or "Snapshots":[]}
     */
    @GET
    @Path("/status/{date}/snapshots")
    @Produces(MediaType.APPLICATION_JSON)
    public Response snapshotsByDate(@PathParam("date") String date) throws Exception {
        List<BackupMetadata> metadata = this.completedBkups.locate(date);
        JSONObject object = new JSONObject();
        List<String> snapshots = new ArrayList<>();

        if (metadata != null && !metadata.isEmpty())
            snapshots.addAll(
                    metadata.stream()
                            .filter(
                                    backupMetadata ->
                                            backupMetadata
                                                    .getBackupVersion()
                                                    .equals(BackupVersion.SNAPSHOT_BACKUP))
                            .map(
                                    backupMetadata ->
                                            DateUtil.formatyyyyMMddHHmm(backupMetadata.getStart()))
                            .collect(Collectors.toList()));

        object.put("Snapshots", snapshots);
        return Response.ok(object.toString(), MediaType.APPLICATION_JSON).build();
    }

    /*
     * Determines the validity of the backup by i) Downloading meta.json file ii) Listing of the backup directory
     * iii) Find the missing or extra files in backup location.
     * This by default takes the latest snapshot of the application. One can provide exact hour and min to check specific backup.
     * Input: Daterange in the format of yyyyMMddHHmm,yyyyMMddHHmm OR yyyyMMdd,yyyyMMdd OR default
     */
    @GET
    @Path("/validate/snapshot/{daterange}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response validateSnapshotByDate(
            @PathParam("daterange") String daterange,
            @DefaultValue("false") @QueryParam("force") boolean force)
            throws Exception {
        DateUtil.DateRange dateRange = new DateUtil.DateRange(daterange);
        Optional<BackupVerificationResult> result =
                backupVerification.verifyLatestBackup(
                        BackupVersion.SNAPSHOT_BACKUP, force, dateRange);
        if (!result.isPresent()) {
            return Response.noContent()
                    .entity("No valid meta found for provided time range")
                    .build();
        }

        return Response.ok(result.get().toString()).build();
    }

    /*
     * A list of files for requested filter.  Currently, the only supported filter is META, all others will be ignore.
     * For filter of META, ONLY the daily snapshot meta file (meta.json)  are accounted for, not the incremental meta file.
     * In addition, we do ONLY list the name of the meta data file, not the list of data files within it.
     *
     * @param handle to the json response
     * @param a list of all files (data (*.db), and meta data file (*.json)) from S3 for requested dates.
     * @param backup meta data file filter.  Currently, the only supported filter is META, all others will be ignore.
     * @return a list of files in Json format.
     */
    private JSONObject constructJsonResponse(
            JSONObject object, Iterator<AbstractBackupPath> it, String filter) throws Exception {
        int fileCnt = 0;
        filter = filter.contains("?") ? filter.substring(0, filter.indexOf("?")) : filter;

        try {
            JSONArray jArray = new JSONArray();
            while (it.hasNext()) {
                AbstractBackupPath p = it.next();
                if (!filter.isEmpty() && BackupFileType.valueOf(filter) != p.getType()) continue;
                JSONObject backupJSON = new JSONObject();
                backupJSON.put("bucket", config.getBackupPrefix());
                backupJSON.put("filename", p.getRemotePath());
                backupJSON.put("app", p.getClusterName());
                backupJSON.put("region", p.getRegion());
                backupJSON.put("token", p.getToken());
                backupJSON.put("ts", DateUtil.formatyyyyMMddHHmm(p.getTime()));
                backupJSON.put(
                        "instance_id", p.getInstanceIdentity().getInstance().getInstanceId());
                backupJSON.put("uploaded_ts", DateUtil.formatyyyyMMddHHmm(p.getUploadedTs()));
                if ("meta".equalsIgnoreCase(filter)) { // only check for existence of meta file
                    p.setFileName(
                            "meta.json"); // ignore incremental meta files, we are only interested
                    // in daily snapshot
                    if (metaData.doesExist(p)) {
                        // if here, snapshot completed.
                        fileCnt++;
                        jArray.put(backupJSON);
                        backupJSON.put("num_files", "1");
                    }
                } else { // account for every file (data, and meta) .
                    fileCnt++;
                    jArray.put(backupJSON);
                }
            }
            object.put("files", jArray);
            object.put("num_files", fileCnt);
        } catch (JSONException jse) {
            logger.info("Caught JSON Exception --> {}", jse.getMessage());
        }
        return object;
    }
}
