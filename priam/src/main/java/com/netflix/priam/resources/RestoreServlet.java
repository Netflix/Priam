package com.netflix.priam.resources;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.CassandraTuner;
import com.netflix.priam.utils.ITokenManager;

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
	private IPriamInstanceFactory factory;
	private CassandraTuner tuner;
	private ICassandraProcess cassProcess;
	private ITokenManager tokenManager;

	@Inject
	public RestoreServlet(IConfiguration config, Restore restoreObj, Provider<AbstractBackupPath> pathProvider, PriamServer priamServer
			, IPriamInstanceFactory factory, CassandraTuner tuner, ICassandraProcess cassProcess, ITokenManager tokenManager) {
		this.config = config;
		this.restoreObj = restoreObj;
		this.pathProvider = pathProvider;
		this.priamServer = priamServer;
		this.factory = factory;
		this.tuner = tuner;
		this.cassProcess = cassProcess;
		this.tokenManager = tokenManager;
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
    public Response	status() throws Exception {
        JSONObject object = new JSONObject();
        Task.STATE state = this.restoreObj.getRestoreState();
        if (state.equals(Task.STATE.NOT_APPLICABLE)) {
            object.put("status", this.restoreObj.getRestoreState());
            object.put("daterange", "NOT_APPLICABLE");
            object.put("starttime", "NOT_APPLICABLE");
            object.put("endtime", "NOT_APPLICABLE");
            object.put("token", "NOT_APPLICABLE");
            
            return Response.status(503).type(MediaType.APPLICATION_JSON)
            	.entity(object.toString(2)).build();
            
        } else if (state.equals(Task.STATE.DONE)) {
        	object.put("status", this.restoreObj.getRestoreState());
            if (this.restoreObj.getStartDateRange() != null && this.restoreObj.getEndDateRange() != null) {
                object.put("daterange", this.restoreObj.getStartDateRange() + "," + this.restoreObj.getEndDateRange());
            }
            if (this.restoreObj.getExecStartTime() != null ) {
            	object.put("starttime", this.restoreObj.getExecStartTime());
            }
            if (this.restoreObj.getExecEndTime() != null ) {
            	object.put("endtime", this.restoreObj.getExecEndTime());
            }
            
            return Response.ok(object.toString(2), MediaType.APPLICATION_JSON).build();
            
        } else if (state.equals(Task.STATE.RUNNING)) {
        	object.put("status", this.restoreObj.getRestoreState());
            if (this.restoreObj.getStartDateRange() != null && this.restoreObj.getEndDateRange() != null) {
                object.put("daterange", this.restoreObj.getStartDateRange() + "," + this.restoreObj.getEndDateRange());
            }
            if (this.restoreObj.getExecStartTime() != null ) {
            	object.put("starttime", this.restoreObj.getExecStartTime());
            }
            if (this.restoreObj.getExecEndTime() != null ) {
            	object.put("endtime", this.restoreObj.getExecEndTime());
            }
            
            return Response.status(206).type(MediaType.APPLICATION_JSON)
                	.entity(object.toString(2)).build();
            
        } else {
        	object.put("status", this.restoreObj.getRestoreState());
            if (this.restoreObj.getStartDateRange() != null && this.restoreObj.getEndDateRange() != null) {
                object.put("daterange", this.restoreObj.getStartDateRange() + "," + this.restoreObj.getEndDateRange());
            }
            if (this.restoreObj.getExecStartTime() != null ) {
            	object.put("starttime", this.restoreObj.getExecStartTime());
            }
            if (this.restoreObj.getExecEndTime() != null ) {
            	object.put("endtime", this.restoreObj.getExecEndTime());
            }
            
            return Response.status(500).type(MediaType.APPLICATION_JSON)
                	.entity(object.toString(2)).build();
        }
        
    }
	
    @GET
    @Path("/restore")
    public Response restore(@QueryParam(REST_HEADER_RANGE) String daterange, @QueryParam(REST_HEADER_REGION) String region, @QueryParam(REST_HEADER_TOKEN) String token,
            @QueryParam(REST_KEYSPACES) String keyspaces, @QueryParam(REST_RESTORE_PREFIX) String restorePrefix) throws Exception
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
        
        String origRestorePrefix = config.getRestorePrefix();
        if (StringUtils.isNotBlank(restorePrefix))
        {
            config.setRestorePrefix(restorePrefix);
        }
        
        logger.info("Parameters: { token: [" + token + "], region: [" +  region + "], startTime: [" + startTime + "], endTime: [" + endTime + 
                    "], keyspaces: [" + keyspaces + "], restorePrefix: [" + restorePrefix + "]}");
        
        restore(token, region, startTime, endTime, keyspaces);
        
        //Since this call is probably never called in parallel, config is multi-thread safe to be edited
        if (origRestorePrefix != null)
                config.setRestorePrefix(origRestorePrefix);       
        else    config.setRestorePrefix(""); 

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
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
        if (StringUtils.isNotBlank(keyspaces))
        {
                 List<String> newKeyspaces = Lists.newArrayList(keyspaces.split(","));
                 config.setRestoreKeySpaces(newKeyspaces);
        }
    }

}