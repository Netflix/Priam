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

import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.backup.IncrementalRestore;
import com.netflix.priam.backup.MetaData;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.CassandraTuner;
import com.netflix.priam.utils.ITokenManager;

@Path("/v1/backup")
@Produces(MediaType.APPLICATION_JSON)
public class BackupServlet
{
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final String REST_HEADER_RANGE = "daterange";
    private static final String REST_HEADER_FILTER = "filter";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_HEADER_REGION = "region";
    private static final String REST_KEYSPACES = "keyspaces";
    private static final String FMT = "yyyyMMddHHmm";

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
    @Inject
    private PriamScheduler scheduler;
    @Inject
    private MetaData metaData;

    @Inject
    public BackupServlet(PriamServer priamServer, IConfiguration config, @Named("backup")IBackupFileSystem backupFs,@Named("backup_status")IBackupFileSystem bkpStatusFs, Restore restoreObj, Provider<AbstractBackupPath> pathProvider, CassandraTuner tuner,
            SnapshotBackup snapshotBackup, IPriamInstanceFactory factory, ITokenManager tokenManager, ICassandraProcess cassProcess)
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
    }

    @GET
    @Path("/do_snapshot")
    public Response backup() throws Exception
    {
        snapshotBackup.execute();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/incremental_backup")
    public Response backupIncrementals() throws Exception
    {
        scheduler.addTask("IncrementalBackup", IncrementalBackup.class, IncrementalBackup.getTimer());
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/restore")
    public Response restore(@QueryParam(REST_HEADER_RANGE) String daterange, @QueryParam(REST_HEADER_REGION) String region, @QueryParam(REST_HEADER_TOKEN) String token,
            @QueryParam(REST_KEYSPACES) String keyspaces) throws Exception
    {
        Date startTime;
        Date endTime;

        if (StringUtils.isBlank(daterange) || daterange.equalsIgnoreCase("default"))
        {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        }
        else
        {
            String[] restore = daterange.split(",");
            AbstractBackupPath path = pathProvider.get();
            startTime = path.parseDate(restore[0]);
            endTime = path.parseDate(restore[1]);
        }
        restore(token, region, startTime, endTime, keyspaces);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }
    
    @GET
    @Path("/incremental_restore")
    public Response restoreIncrementals() throws Exception
    {
        scheduler.addTask(IncrementalRestore.JOBNAME, IncrementalRestore.class, IncrementalRestore.getTimer());
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Path("/list")
    public Response list(@QueryParam(REST_HEADER_RANGE) String daterange, @QueryParam(REST_HEADER_FILTER) String filter) throws Exception
    {
        Date startTime;
        Date endTime;

        if (StringUtils.isBlank(daterange) || daterange.equalsIgnoreCase("default"))
        {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        }
        else
        {
            String[] restore = daterange.split(",");
            AbstractBackupPath path = pathProvider.get();
            startTime = path.parseDate(restore[0]);
            endTime = path.parseDate(restore[1]);
        }
        Iterator<AbstractBackupPath> it = bkpStatusFs.list(config.getBackupPrefix(), startTime, endTime);
        JSONObject object = new JSONObject();
        object = constructJsonResponse(object,it,filter);
//        while (it.hasNext())
//        {
//            AbstractBackupPath p = it.next();
//            if (filter != null && BackupFileType.valueOf(filter) != p.getType())
//                continue;
//            object = constructJsonResponse(object,p);
////            object.put(p.getRemotePath(), p.formatDate(p.getTime()));
//        }
        return Response.ok(object.toString(2), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/status")
    public Response status() throws Exception
    {
        int restoreTCount = restoreObj.getActiveCount();
        logger.debug("Thread counts for backup is: %d", restoreTCount);
        int backupTCount = backupFs.getActivecount();
        logger.debug("Thread counts for restore is: %d", backupTCount);
        JSONObject object = new JSONObject();
        object.put("Restore", new Integer(restoreTCount));
        object.put("Status", restoreObj.state().toString());
        object.put("Backup", new Integer(backupTCount));
        object.put("Status", snapshotBackup.state().toString());
        return Response.ok(object.toString(), MediaType.APPLICATION_JSON).build();
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
    private void restore(String token, String region, Date startTime, Date endTime, String keyspaces) throws Exception
    {
        String origRegion = config.getDC();
        String origToken = priamServer.getId().getInstance().getToken();
        if (StringUtils.isNotBlank(token))
            priamServer.getId().getInstance().setToken(token);

        if( config.isRestoreClosestToken())
            priamServer.getId().getInstance().setToken(closestToken(priamServer.getId().getInstance().getToken(), config.getDC()));
        
        if (StringUtils.isNotBlank(region))
        {
            config.setDC(region);
            logger.info("Restoring from region " + region);
            priamServer.getId().getInstance().setToken(closestToken(priamServer.getId().getInstance().getToken(), region));
            logger.info("Restore will use token " + priamServer.getId().getInstance().getToken());
        }

        setRestoreKeyspaces(keyspaces);

        try
        {
            restoreObj.restore(startTime, endTime);
        }
        finally
        {
            config.setDC(origRegion);
            priamServer.getId().getInstance().setToken(origToken);
        }
        tuner.updateAutoBootstrap(config.getYamlLocation(), false);
        cassProcess.start(true);
    }

    /**
     * Find closest token in the specified region
     */
    private String closestToken(String token, String region)
    {
        List<PriamInstance> plist = factory.getAllIds(config.getAppName());
        List<BigInteger> tokenList = Lists.newArrayList();
        for (PriamInstance ins : plist)
        {
            if (ins.getDC().equalsIgnoreCase(region))
                tokenList.add(new BigInteger(ins.getToken()));
        }
        return tokenManager.findClosestToken(new BigInteger(token), tokenList).toString();
    }

    /*
     * TODO: decouple the servlet, config, and restorer. this should not rely on a side
     *       effect of a list mutation on the config object (treating it as global var).
     */
    private void setRestoreKeyspaces(String keyspaces)
    {
        List<String> list = config.getRestoreKeySpaces();
        list.clear();
        if (keyspaces != null)
        {
            logger.info("Restoring keyspaces: " + keyspaces);
            List<String> newKeyspaces = Lists.newArrayList(keyspaces.split(","));
            list.addAll(newKeyspaces);
        }
    }
    
    private JSONObject constructJsonResponse(JSONObject object, Iterator<AbstractBackupPath> it,String filter) throws Exception
    {
    		String logPrefix = "000000";
    		int logCnt = 1;
    		filter  = filter.contains("?") ? filter.substring(0, filter.indexOf("?")) : filter;
    	    Map<String,String> keyValues = new LinkedHashMap<String,String>();
//    	    while (it.hasNext())
//        {
//            AbstractBackupPath p = it.next();
//            if (filter != null && BackupFileType.valueOf(filter) != p.getType())
//                continue;
//            keyValues.put("BUCKET", config.getBackupPrefix());
//            keyValues.put("FILENAME", p.getRemotePath());
//            keyValues.put("APP", p.getClusterName());
//            keyValues.put("REGION", p.getRegion());
//            keyValues.put("TOKEN", p.getToken());
//            keyValues.put("TIMESTAMP", new DateTime(p.getTime()).toString(FMT));
//            keyValues.put("INSTANCE-ID", p.getInstanceIdentity().getInstance().getInstanceId());
//            keyValues.put("UPLOADED-TS", new DateTime(p.getUploadedTs()).toString(FMT));
//            if(filter != null && filter.equalsIgnoreCase("meta"))
//            {
//            		List<AbstractBackupPath> allFiles = metaData.get(p);
//            		long totalSize = 0;
//            		for(AbstractBackupPath abp:allFiles)
//            			totalSize = totalSize + abp.getSize();
//            		keyValues.put("NUM-FILES", Long.toString(allFiles.size()));
////            		keyValues.put("TOTAL-SIZE", Long.toString(totalSize)); // Add Later
//            }
//            object.put("File ("+logPrefix+logCnt+++")", keyValues);
//        }
    	    int fileCnt = 0;
//    	    JSONObject jObject = new JSONObject();
            try {
                    JSONArray jArray = new JSONArray();
            	    while (it.hasNext())
                    {
            	    			JSONObject backupJSON = new JSONObject();
            	    			AbstractBackupPath p = it.next();
                        if (filter != null && BackupFileType.valueOf(filter) != p.getType())
                            continue;
                        backupJSON.put("bucket", config.getBackupPrefix());
                        backupJSON.put("filename", p.getRemotePath());
                        backupJSON.put("app", p.getClusterName());
                        backupJSON.put("region", p.getRegion());
                        backupJSON.put("token", p.getToken());
                        backupJSON.put("ts", new DateTime(p.getTime()).toString(FMT));
                        backupJSON.put("instance_id", p.getInstanceIdentity().getInstance().getInstanceId());
                        backupJSON.put("uploaded_ts", new DateTime(p.getUploadedTs()).toString(FMT));
                        if(filter != null && filter.equalsIgnoreCase("meta"))
                        {
                        		List<AbstractBackupPath> allFiles = metaData.get(p);
                        		long totalSize = 0;
                        		for(AbstractBackupPath abp:allFiles)
                        			totalSize = totalSize + abp.getSize();
                        		backupJSON.put("num_files", Long.toString(allFiles.size()));
//                        		keyValues.put("TOTAL-SIZE", Long.toString(totalSize)); // Add Later
                        }
                        fileCnt++;
                        jArray.put(backupJSON);
                    }
                    object.put("files", jArray);
                    object.put("num_files", fileCnt);
            } catch (JSONException jse) {
                //jse.printStacktrace();
            }
   		return object;
    }
}
