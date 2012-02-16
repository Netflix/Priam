package com.netflix.priam.resources;

import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.json.JSONObject;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TuneCassandra;

@Path("/v1/backup/{type}")
@Produces({ "application/text" })
public class BackupServlet
{
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    private static final String REST_HEADER_KEY = "type";
    private static final String REST_HEADER_RANGE = "daterange";
    private static final String REST_HEADER_FILTER = "filter";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_HEADER_REGION = "region";
    private static final String REST_KEYSPACES = "keyspaces";

    private PriamServer priamServer;
    private IConfiguration config;
    private IBackupFileSystem fs;
    private Restore restoreObj;
    private Provider<AbstractBackupPath> pathProvider;
    private TuneCassandra tuneCassandra;
    private SnapshotBackup snapshotBackup;
    private IPriamInstanceFactory factory;

    @Inject
    public BackupServlet(PriamServer priamServer, IConfiguration config, IBackupFileSystem fs, Restore restoreObj, Provider<AbstractBackupPath> pathProvider, TuneCassandra tunecassandra,
            SnapshotBackup snapshotBackup, IPriamInstanceFactory factory)
    {
        this.priamServer = priamServer;
        this.config = config;
        this.fs = fs;
        this.restoreObj = restoreObj;
        this.pathProvider = pathProvider;
        this.tuneCassandra = tunecassandra;
        this.snapshotBackup = snapshotBackup;
        this.factory = factory;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response restore(@PathParam(REST_HEADER_KEY) String type, @QueryParam(REST_HEADER_RANGE) String daterange, @QueryParam(REST_HEADER_FILTER) String filter,
            @QueryParam(REST_HEADER_REGION) String region, @QueryParam(REST_HEADER_TOKEN) String token, @QueryParam(REST_KEYSPACES) String keyspaces) throws Exception
    {
        Date startTime;
        Date endTime;

        String[] restore = daterange.split(",");
        if (daterange.equalsIgnoreCase("default"))
        {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        }
        else
        {
            AbstractBackupPath path = pathProvider.get();
            startTime = path.getFormat().parse(restore[0]);
            endTime = path.getFormat().parse(restore[1]);
        }
        if (type.equalsIgnoreCase("restore"))
        {
            restore(token, region, startTime, endTime, keyspaces);
            return Response.ok("Restore completed").build();
        }
        else if (type.equalsIgnoreCase("list"))
        {
            Iterator<AbstractBackupPath> it = fs.list(config.getBackupPrefix(), startTime, endTime);
            JSONObject object = new JSONObject();
            while (it.hasNext())
            {
                AbstractBackupPath p = it.next();
                if (filter != null && BackupFileType.valueOf(filter) != p.getType())
                    continue;
                object.put(p.getRemotePath(), p.getFormat().format(p.getTime()));
            }
            return Response.ok(object.toString()).build();
        }
        else if (type.equalsIgnoreCase("do_snapshot"))
        {
            snapshotBackup.execute();
            return Response.status(200).build();
        }
        else if (type.equalsIgnoreCase("status"))
        {
            int restoreTCount = restoreObj.getActiveCount();
            logger.debug("Thread counts for backup is: %d", restoreTCount);
            int backupTCount = fs.getActivecount();
            logger.debug("Thread counts for restore is: %d", backupTCount);
            return Response.ok("Restore: " + restoreTCount + "\nStatus: " + restoreObj.state() + "\nBackup: " + backupTCount + "\nStatus: " + snapshotBackup.state()).build();
        }

        logger.error(String.format("Couldnt serve the URL with parameters: type=%s and ext=%s", type, daterange));
        return Response.status(404).build();
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
        String origToken = priamServer.getId().getInstance().getPayload();
        if (token != null && token != "")
            priamServer.getId().getInstance().setPayload(token);

        if (StringUtils.isNotBlank(region))
        {
            config.setDC(region);
            logger.info("Restoring from region " + region);
            priamServer.getId().getInstance().setPayload(closestToken(priamServer.getId().getInstance().getPayload(), region));
            logger.info("Restore will use token " + priamServer.getId().getInstance().getPayload());
        }

        setRestoreKeyspaces(keyspaces);

        try
        {
            restoreObj.restore(startTime, endTime);
        }
        finally
        {
            config.setDC(origRegion);
            priamServer.getId().getInstance().setPayload(origToken);
        }
        tuneCassandra.updateYaml(false);
        SystemUtils.startCassandra(true, config);
    }

    /**
     * Find closest token in the specified region
     * 
     * @param token
     * @param region
     * @return
     */
    private String closestToken(String token, String region)
    {
        List<PriamInstance> plist = factory.getAllIds(config.getAppName());
        List<BigInteger> tokenList = Lists.newArrayList();
        for (PriamInstance ins : plist)
        {
            if (ins.getDC().equalsIgnoreCase(region))
                tokenList.add(new BigInteger(ins.getPayload()));
        }
        return TokenManager.findClosestToken(new BigInteger(token), tokenList).toString();
    }

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
}
