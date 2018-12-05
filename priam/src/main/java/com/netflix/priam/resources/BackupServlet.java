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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.priam.backup.*;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.SystemUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
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
    private final IBackupFileSystem backupFs;
    private final SnapshotBackup snapshotBackup;
    private final BackupVerification backupVerification;
    @Inject private PriamScheduler scheduler;
    private final IBackupStatusMgr completedBkups;
    @Inject private MetaData metaData;

    @Inject
    public BackupServlet(
            IConfiguration config,
            @Named("backup") IBackupFileSystem backupFs,
            SnapshotBackup snapshotBackup,
            IBackupStatusMgr completedBkups,
            BackupVerification backupVerification) {
        this.config = config;
        this.backupFs = backupFs;
        this.snapshotBackup = snapshotBackup;
        this.completedBkups = completedBkups;
        this.backupVerification = backupVerification;
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
                "IncrementalBackup", IncrementalBackup.class, IncrementalBackup.getTimer());
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
        JSONObject object = new JSONObject();
        List<BackupMetadata> metadataLinkedList = this.completedBkups.locate(date);

        if (metadataLinkedList != null && !metadataLinkedList.isEmpty()) {

            // backup exist base on requested date, lets fetch more of its metadata
            BackupMetadata bkupMetadata = metadataLinkedList.get(0);
            object.put("Snapshotstatus", bkupMetadata.getStatus().equals(Status.FINISHED));

            String token = bkupMetadata.getToken();
            if (token != null && !token.isEmpty()) {
                object.put("token", bkupMetadata.getToken());
            } else {
                object.put("token", "not available");
            }
            if (bkupMetadata.getStart() != null) {
                object.put("starttime", DateUtil.formatyyyyMMddHHmm(bkupMetadata.getStart()));
            } else {
                object.put("starttime", "not available");
            }

            if (bkupMetadata.getCompleted() != null) {
                object.put(
                        "completetime", DateUtil.formatyyyyMMddHHmm(bkupMetadata.getCompleted()));
            } else {
                object.put("completetime", "not_available");
            }

        } else { // Backup do not exist for that date.
            object.put("Snapshotstatus", false);
            String token =
                    SystemUtils.getDataFromUrl(
                            "http://localhost:8080/Priam/REST/v1/cassconfig/get_token");
            if (token != null && !token.isEmpty()) {
                object.put("token", token);
            } else {
                object.put("token", "not available");
            }
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
                            .map(
                                    backupMetadata ->
                                            DateUtil.formatyyyyMMddHHmm(backupMetadata.getStart()))
                            .collect(Collectors.toList()));

        object.put("Snapshots", snapshots);
        return Response.ok(object.toString(), MediaType.APPLICATION_JSON).build();
    }

    private List<BackupMetadata> getLatestBackupMetadata(DateUtil.DateRange dateRange) {
        Date startTime = Date.from(dateRange.getStartTime());
        Date endTime = Date.from(dateRange.getEndTime());
        List<BackupMetadata> backupMetadata = this.completedBkups.locate(endTime);
        if (backupMetadata != null && !backupMetadata.isEmpty()) return backupMetadata;
        if (DateUtil.formatyyyyMMdd(startTime).equals(DateUtil.formatyyyyMMdd(endTime))) {
            logger.info(
                    "Start & end date are same. No SNAPSHOT found for date: {}",
                    DateUtil.formatyyyyMMdd(endTime));
            return null;
        } else {
            Date previousDay = new Date(endTime.getTime());
            do {
                // We need to find the latest backupmetadata in this date range.
                previousDay = new DateTime(previousDay.getTime()).minusDays(1).toDate();
                logger.info(
                        "Will try to find snapshot for previous day: {}",
                        DateUtil.formatyyyyMMdd(previousDay));
                backupMetadata = completedBkups.locate(previousDay);
                if (backupMetadata != null && !backupMetadata.isEmpty()) return backupMetadata;
            } while (!DateUtil.formatyyyyMMdd(startTime)
                    .equals(DateUtil.formatyyyyMMdd(previousDay)));
        }
        return null;
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
    public Response validateSnapshotByDate(@PathParam("daterange") String daterange)
            throws Exception {

        DateUtil.DateRange dateRange = new DateUtil.DateRange(daterange);

        JSONObject jsonReply = new JSONObject();
        jsonReply.put(
                "inputStartDate",
                DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, dateRange.getStartTime()));
        jsonReply.put(
                "inputEndDate",
                DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, dateRange.getEndTime()));
        logger.info(
                "Will try to validate latest backup during startTime: {}, and endTime: {}",
                dateRange.getStartTime(),
                dateRange.getEndTime());

        List<BackupMetadata> metadata = getLatestBackupMetadata(dateRange);
        BackupVerificationResult result =
                backupVerification.verifyBackup(metadata, Date.from(dateRange.getStartTime()));
        jsonReply.put("snapshotAvailable", result.snapshotAvailable);
        jsonReply.put("valid", result.valid);
        jsonReply.put("backupFileListAvailable", result.backupFileListAvail);
        jsonReply.put("metaFileFound", result.metaFileFound);
        jsonReply.put("selectedDate", result.selectedDate);
        jsonReply.put("snapshotTime", result.snapshotTime);
        jsonReply.put("filesInMetaOnly", result.filesInMetaOnly);
        jsonReply.put("filesInS3Only", result.filesInS3Only);
        jsonReply.put("filesMatched", result.filesMatched);
        return Response.ok(jsonReply.toString()).build();
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
