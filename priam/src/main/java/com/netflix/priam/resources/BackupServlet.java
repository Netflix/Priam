/**
 * Copyright 2013 Netflix, Inc.
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
import com.google.inject.name.Named;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.*;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.CassandraTuner;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.SystemUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

@Path("/v1/backup")
@Produces(MediaType.APPLICATION_JSON)
public class BackupServlet {
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final String REST_HEADER_RANGE = "daterange";
    private static final String REST_HEADER_FILTER = "filter";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_HEADER_REGION = "region";
    private static final String REST_KEYSPACES = "keyspaces";
    private static final String REST_RESTORE_PREFIX = "restoreprefix";
    private static final String FMT = "yyyyMMddHHmm";
    private static final String REST_LOCR_ROWKEY = "verifyrowkey";
    private static final String REST_LOCR_KEYSPACE = "verifyks";
    private static final String REST_LOCR_COLUMNFAMILY = "verifycf";
    private static final String REST_LOCR_FILEEXTENSION = "verifyfileextension";
    private static final String SSTABLE2JSON_DIR_LOCATION = "/tmp/priam_sstables";
    private static final String SSTABLE2JSON_COMMAND_FROM_CASSHOME = "/bin/sstable2json";

    private PriamServer priamServer;
    private IConfiguration config;
    private IBackupFileSystem backupFs;
    private IBackupFileSystem bkpStatusFs;
    private Restore restoreObj;
    private Provider<AbstractBackupPath> pathProvider;
    private CassandraTuner tuner;
    private SnapshotBackup snapshotBackup;
    private IPriamInstanceFactory factory;
    private final ITokenManager tokenManager;
    private final ICassandraProcess cassProcess;
    private BackupVerification backupVerification;
    @Inject
    private PriamScheduler scheduler;
    @Inject
    private MetaData metaData;

    private IBackupStatusMgr completedBkups;
    private SimpleDateFormat simpleDateFormatDate = new SimpleDateFormat("yyyyMMdd");

    @Inject

    public BackupServlet(PriamServer priamServer, IConfiguration config, @Named("backup") IBackupFileSystem backupFs, @Named("backup_status") IBackupFileSystem bkpStatusFs, Restore restoreObj, Provider<AbstractBackupPath> pathProvider, CassandraTuner tuner,
                         SnapshotBackup snapshotBackup, IPriamInstanceFactory factory, ITokenManager tokenManager, ICassandraProcess cassProcess
            , @Named("backupStatusMgr") IBackupStatusMgr completedBkups, BackupVerification backupVerification)

    {
        this.priamServer = priamServer;
        this.config = config;
        this.backupFs = backupFs;
        this.bkpStatusFs = bkpStatusFs;
        this.restoreObj = restoreObj;
        this.pathProvider = pathProvider;
        this.tuner = tuner;
        this.snapshotBackup = snapshotBackup;
        this.factory = factory;
        this.tokenManager = tokenManager;
        this.cassProcess = cassProcess;
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
        scheduler.addTask("IncrementalBackup", IncrementalBackup.class, IncrementalBackup.getTimer());
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Path("/list")
    /*
     * Fetch the list of files for the requested date range.
     * 
     * @param date range
     * @param filter.  The type of data files fetched.  E.g. META will only fetch the dailsy snapshot meta data file (meta.json).
     * @return the list of files in json format as part of the Http response body.
     */
    public Response list(@QueryParam(REST_HEADER_RANGE) String daterange, @QueryParam(REST_HEADER_FILTER) @DefaultValue("") String filter) throws Exception {
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

        logger.info("Parameters: {backupPrefix: [" + config.getBackupPrefix() + "], daterange: [" + daterange + "], filter: [" + filter + "]}");

        Iterator<AbstractBackupPath> it = bkpStatusFs.list(config.getBackupPrefix(), startTime, endTime);
        JSONObject object = new JSONObject();
        object = constructJsonResponse(object, it, filter);
        return Response.ok(object.toString(2), MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response status() throws Exception {
        int restoreTCount = restoreObj.getActiveCount(); //Active threads performing the restore
        logger.debug("Thread counts for restore is: %d", restoreTCount);
        int backupTCount = backupFs.getActivecount();
        logger.debug("Thread counts for snapshot backup is: %d", backupTCount);
        JSONObject object = new JSONObject();
        object.put("Restore", new Integer(restoreTCount)); //Number of active threads performing the restore
        object.put("status", restoreObj.state().toString()); //state of the restore [ERROR|RUNNING|DONE]
        object.put("Backup", new Integer(backupTCount)); //Number of active threads performing the snapshot backups
        object.put("Snapshotstatus", snapshotBackup.state().toString());
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
        BackupMetadata bkupMetadata = this.completedBkups.locate(IMessageObserver.BACKUP_MESSAGE_TYPE.SNAPSHOT, date);

        if (bkupMetadata != null) { //backup exist base on requested date, lets fetch more of its metadata
            object.put("Snapshotstatus", true);
            String token = bkupMetadata.getToken();
            if (token != null && !token.isEmpty()) {
                object.put("token", bkupMetadata.getToken());
            } else {
                object.put("token", "not available");
            }
            if (bkupMetadata.getStartTime() != null) {
                object.put("starttime", SystemUtils.formatDate(bkupMetadata.getStartTime(), "yyyyMMddHHmm"));
            } else {
                object.put("starttime", "not available");
            }

            Date completeTime = bkupMetadata.getCompletedTime();
            if (bkupMetadata.getCompletedTime() != null) {
                object.put("completetime", SystemUtils.formatDate(bkupMetadata.getCompletedTime(), "yyyyMMddHHmm"));
            } else {
                object.put("completetime", "not_available");
            }

        } else { //Backup do not exist for that date.
            object.put("Snapshotstatus", false);
            String token = SystemUtils.getDataFromUrl("http://localhost:8080/Priam/REST/v1/cassconfig/get_token");
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
        BackupMetadata metadata = this.completedBkups.locate(IMessageObserver.BACKUP_MESSAGE_TYPE.SNAPSHOT, date);
        JSONObject object = new JSONObject();
        List<String> snapshots = new ArrayList<String>();

        if (metadata != null)
            snapshots.addAll(metadata.getBackups());

        object.put("Snapshots", snapshots);
        return Response.ok(object.toString(), MediaType.APPLICATION_JSON).build();
    }

    private BackupMetadata getLatestBackupMetadata(Date startTime, Date endTime) {
        BackupMetadata backupMetadata = this.completedBkups.locate(IMessageObserver.BACKUP_MESSAGE_TYPE.SNAPSHOT, simpleDateFormatDate.format(endTime));
        if (backupMetadata != null)
            return backupMetadata;
        if (simpleDateFormatDate.format(startTime).equals(simpleDateFormatDate.format(endTime))) {
            logger.info("Start & end date are same. No SNAPSHOT found for date: " + simpleDateFormatDate.format(endTime));
            return null;
        } else {
            Date previousDay = new Date(endTime.getTime());
            do {
                //We need to find the latest backupmetadata in this date range.
                previousDay = new DateTime(previousDay.getTime()).minusDays(1).toDate();
                logger.info("Will try to find snapshot for previous day: " + simpleDateFormatDate.format(previousDay));
                backupMetadata = completedBkups.locate(IMessageObserver.BACKUP_MESSAGE_TYPE.SNAPSHOT, simpleDateFormatDate.format(previousDay));
                if (backupMetadata != null)
                    return backupMetadata;
            } while (!simpleDateFormatDate.format(startTime).equals(simpleDateFormatDate.format(previousDay)));
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
    public Response validateSnapshotByDate(@PathParam("daterange") String daterange) throws Exception {

        Date startTime;
        Date endTime;

        if (StringUtils.isBlank(daterange) || daterange.equalsIgnoreCase("default")) {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        } else {
            String[] dates = daterange.split(",");
            startTime = SystemUtils.getDate(dates[0]);
            endTime = SystemUtils.getDate(dates[1]);
        }

        //Date parsedInputDateTime = getDate(date);
        JSONObject jsonReply = new JSONObject();
        jsonReply.put("inputStartDate", SystemUtils.formatDate(startTime, "yyyyMMddHHmm"));
        jsonReply.put("inputEndDate", SystemUtils.formatDate(endTime, "yyyyMMddHHmm"));
        logger.info("Will try to validate latest backup during startTime: " + simpleDateFormatDate.format(startTime) + ", and endTime: " + simpleDateFormatDate.format(endTime));

        BackupMetadata metadata = getLatestBackupMetadata(startTime, endTime);
        BackupVerificationResult result = backupVerification.verifyBackup(metadata, startTime);
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

    /**
     * <p>
     * Life_Of_C*Row : With this REST call, mutations/existence of a rowkey can be found.
     * It uses SSTable2Json utility which will convert SSTables on disk to JSON format and 
     * Search for the desired rowkey.
     *
     * Steps include:
     * 1. Restoring data for given data range and other params
     * 2. Searching provided rowkey in SSTables and writing search result to JSON
     * 3. Delete all the files under Keyspace Directory.
     *    Deletion is done for efficient space usage, so that same node can be reused for
     *    subsequent runs. 
     * <p>
     *
     * @param: Similar to Restore call and few additional params.
     *
     *      daterange 		: Can not be Null or Default. Comma separated Start & End date eg. 201311250000,201311260000
     *      rowkey    		: rowkey to search (In Hex format)
     *      ks        		: keyspace of mentioned rowkey
     *      cf        		: column family of mentioned rowkey
     *      fileExtension 	: Part of SSTable Data file names 
     *      					  eg. if file name = KS1-CF1-hf-100-Data.db
     *      						  then fileExtension = KS1-CF1-hf
     *
     * @return Creates JSON file based on the passed date at hardcoded dir location : /tmp/priam_sstables
     * 		   If rowkey is not found in the SSTable, JSON file will be empty.
     */
    @GET
    @Path("/life_of_crow")
    @Produces(MediaType.APPLICATION_JSON)
    public Response restore_verify_key(
            @QueryParam(REST_HEADER_RANGE) String daterange,
            @QueryParam(REST_HEADER_REGION) String region,
            @QueryParam(REST_HEADER_TOKEN) String token,
            @QueryParam(REST_KEYSPACES) String keyspaces,
            @QueryParam(REST_RESTORE_PREFIX) String restorePrefix,
            @QueryParam(REST_LOCR_ROWKEY) String rowkey,
            @QueryParam(REST_LOCR_KEYSPACE) String ks,
            @QueryParam(REST_LOCR_COLUMNFAMILY) String cf,
            @QueryParam(REST_LOCR_FILEEXTENSION) String fileExtension) throws Exception {

        Date startTime;
        Date endTime;
        //Creating Dir for Json storage
        SystemUtils.createDirs(SSTABLE2JSON_DIR_LOCATION);
        String JSON_FILE_PATH = "";

        try {

            if (StringUtils.isBlank(daterange)
                    || daterange.equalsIgnoreCase("default")) {
                return Response.ok("\n[\"daterange can't be blank or default.eg.201311250000,201311260000\"]\n", MediaType.APPLICATION_JSON)
                        .build();
            }

            String[] restore = daterange.split(",");
            AbstractBackupPath path = pathProvider.get();
            startTime = path.parseDate(restore[0]);
            endTime = path.parseDate(restore[1]);

            String origRestorePrefix = config.getRestorePrefix();
            if (StringUtils.isNotBlank(restorePrefix)) {
                config.setRestorePrefix(restorePrefix);
            }


            restore(token, region, startTime, endTime, keyspaces);

            // Since this call is probably never called in parallel, config is
            // multi-thread safe to be edited
            config.setRestorePrefix(origRestorePrefix);

            while (!CassandraMonitor.isCassadraStarted())
                Thread.sleep(1000l);

            // initialize json file name
            JSON_FILE_PATH = daterange.split(",")[0].substring(0, 8) + ".json";

            //Convert SSTable2Json and search for given rowkey
            checkSSTablesForKey(rowkey, ks, cf, fileExtension, JSON_FILE_PATH);

        } catch (Exception e) {
            logger.info(ExceptionUtils.getFullStackTrace(e));
        } finally {
            removeAllDataFiles(ks);
        }

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON)
                .build();
    }

    /**
     * Restore with the specified start and end time.
     *
     * @param token
     *            Overrides the current token with this one, if specified
     * @param region
     *            Override the region for searching backup
     * @param startTime
     *            Start time
     * @param endTime
     *            End time upto which the restore should fetch data
     * @param keyspaces
     *            Comma seperated list of keyspaces to restore
     * @throws Exception
     */
    private void restore(String token, String region, Date startTime, Date endTime, String keyspaces) throws Exception {
        String origRegion = config.getDC();
        String origToken = priamServer.getId().getInstance().getToken();
        if (StringUtils.isNotBlank(token))
            priamServer.getId().getInstance().setToken(token);

        if (config.isRestoreClosestToken())
            priamServer.getId().getInstance().setToken(closestToken(priamServer.getId().getInstance().getToken(), config.getDC()));

        if (StringUtils.isNotBlank(region)) {
            config.setDC(region);
            logger.info("Restoring from region " + region);
            priamServer.getId().getInstance().setToken(closestToken(priamServer.getId().getInstance().getToken(), region));
            logger.info("Restore will use token " + priamServer.getId().getInstance().getToken());
        }

        setRestoreKeyspaces(keyspaces);

        try {
            restoreObj.restore(startTime, endTime);
        } finally {
            config.setDC(origRegion);
            priamServer.getId().getInstance().setToken(origToken);
        }
        tuner.updateAutoBootstrap(config.getYamlLocation(), false);
        cassProcess.start(true);
    }

    /**
     * Find closest token in the specified region
     */
    private String closestToken(String token, String region) {
        List<PriamInstance> plist = factory.getAllIds(config.getAppName());
        List<BigInteger> tokenList = Lists.newArrayList();
        for (PriamInstance ins : plist) {
            if (ins.getDC().equalsIgnoreCase(region))
                tokenList.add(new BigInteger(ins.getToken()));
        }
        return tokenManager.findClosestToken(new BigInteger(token), tokenList).toString();
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
    private JSONObject constructJsonResponse(JSONObject object, Iterator<AbstractBackupPath> it, String filter) throws Exception {
        int fileCnt = 0;
        filter = filter.contains("?") ? filter.substring(0, filter.indexOf("?")) : filter;

        try {
            JSONArray jArray = new JSONArray();
            while (it.hasNext()) {
                AbstractBackupPath p = it.next();
                if (!filter.isEmpty() && BackupFileType.valueOf(filter) != p.getType())
                    continue;
                JSONObject backupJSON = new JSONObject();
                backupJSON.put("bucket", config.getBackupPrefix());
                backupJSON.put("filename", p.getRemotePath());
                backupJSON.put("app", p.getClusterName());
                backupJSON.put("region", p.getRegion());
                backupJSON.put("token", p.getToken());
                backupJSON.put("ts", new DateTime(p.getTime()).toString(FMT));
                backupJSON.put("instance_id", p.getInstanceIdentity()
                        .getInstance().getInstanceId());
                backupJSON.put("uploaded_ts",
                        new DateTime(p.getUploadedTs()).toString(FMT));
                if ("meta".equalsIgnoreCase(filter)) { //only check for existence of meta file
                    p.setFileName("meta.json"); //ignore incremental meta files, we are only interested in daily snapshot
                    if (metaData.doesExist(p)) {
                        //if here, snapshot completed.
                        fileCnt++;
                        jArray.put(backupJSON);
                        backupJSON.put("num_files", "1");
                    }
                } else { //account for every file (data, and meta) .
                    fileCnt++;
                    jArray.put(backupJSON);
                }

            }
            object.put("files", jArray);
            object.put("num_files", fileCnt);
        } catch (JSONException jse) {
            logger.info("Caught JSON Exception --> " + jse.getMessage());
        }
        return object;
    }

    /**
     * Convert SSTable2Json and search for given key
     */
    public void checkSSTablesForKey(String rowkey, String keyspace, String cf, String fileExtension, String jsonFilePath) throws Exception {
        try {
            logger.info("Starting SSTable2Json conversion ...");
            //Setting timeout to 10 Mins
            long TIMEOUT_PERIOD = 10l;
            String unixCmd = formulateCommandToRun(rowkey, keyspace, cf, fileExtension, jsonFilePath);

            String[] cmd = {"/bin/sh", "-c", unixCmd.toString()};
            final Process p = Runtime
                    .getRuntime()
                    .exec(cmd);

            Callable<Integer> callable = new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    int returnCode = p.waitFor();
                    return returnCode;
                }
            };

            ExecutorService exeService = Executors.newSingleThreadExecutor();
            try {
                Future<Integer> future = exeService.submit(callable);
                int returnVal = future.get(TIMEOUT_PERIOD, TimeUnit.MINUTES);
                if (returnVal == 0)
                    logger.info("Finished SSTable2Json conversion and search.");
                else
                    logger.error("Error occurred during SSTable2Json conversion and search.");
            } catch (TimeoutException e) {
                logger.error(ExceptionUtils.getFullStackTrace(e));
                throw e;
            } finally {
                p.destroy();
                exeService.shutdown();
            }

        } catch (IOException e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
        }
    }

    public String formulateCommandToRun(String rowkey, String keyspace, String cf, String fileExtension, String jsonFilePath) {
        StringBuffer sbuff = new StringBuffer();

        sbuff.append("for i in $(ls " + config.getDataFileLocation() + File.separator + keyspace + File.separator + cf + File.separator + fileExtension + "-*-Data.db); do " + config.getCassHome() + SSTABLE2JSON_COMMAND_FROM_CASSHOME + " $i -k ");
        sbuff.append(rowkey);
        sbuff.append("  | grep ");
        sbuff.append(rowkey);
        sbuff.append(" >> ");
        sbuff.append(SSTABLE2JSON_DIR_LOCATION + File.separator + jsonFilePath);
        sbuff.append(" ; done");

        logger.info("SSTable2JSON location <" + SSTABLE2JSON_DIR_LOCATION + File.separator + jsonFilePath + ">");
        logger.info("Running Command = " + sbuff.toString());
        return sbuff.toString();
    }

    public void removeAllDataFiles(String ks) throws Exception {
        String cleanupDirPath = config.getDataFileLocation() + File.separator + ks;
        logger.info("Starting to clean all the files inside <" + cleanupDirPath + ">");
        SystemUtils.cleanupDir(cleanupDirPath, null);
        logger.info("*** Done cleaning all the files inside <" + cleanupDirPath + ">");
    }

}