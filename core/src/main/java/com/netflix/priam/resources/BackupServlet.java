package com.netflix.priam.resources;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IncrementalRestore;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TuneCassandra;
import org.apache.commons.lang.StringUtils;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Path ("/v1/backup")
@Produces (MediaType.APPLICATION_JSON)
public class BackupServlet {
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final String REST_HEADER_RANGE = "daterange";
    private static final String REST_HEADER_FILTER = "filter";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_HEADER_REGION = "region";
    private static final String REST_KEYSPACES = "keyspaces";

    private PriamServer priamServer;
    private IBackupFileSystem fs;
    private Restore restoreObj;
    private Provider<AbstractBackupPath> pathProvider;
    private TuneCassandra tuneCassandra;
    private SnapshotBackup snapshotBackup;
    private IPriamInstanceRegistry instanceRegistry;
    @Inject
    private PriamScheduler scheduler;

    private CassandraConfiguration cassandraConfiguration;
    private AmazonConfiguration amazonConfiguration;
    private BackupConfiguration backupConfiguration;


    @Inject
    public BackupServlet(PriamServer priamServer,
                         CassandraConfiguration cassandraConfiguration,
                         AmazonConfiguration amazonConfiguration,
                         BackupConfiguration backupConfiguration,
                         IBackupFileSystem fs,
                         Restore restoreObj,
                         Provider<AbstractBackupPath> pathProvider,
                         TuneCassandra tunecassandra,
                         SnapshotBackup snapshotBackup,
                         IPriamInstanceRegistry instanceRegistry) {
        this.priamServer = priamServer;
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.backupConfiguration = backupConfiguration;
        this.fs = fs;
        this.restoreObj = restoreObj;
        this.pathProvider = pathProvider;
        this.tuneCassandra = tunecassandra;
        this.snapshotBackup = snapshotBackup;
        this.instanceRegistry = instanceRegistry;
    }

    @GET
    @Path ("/do_snapshot")
    public Response backup() throws Exception {
        snapshotBackup.execute();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/restore")
    public Response restore(@QueryParam (REST_HEADER_RANGE) String daterange, @QueryParam (REST_HEADER_REGION) String region, @QueryParam (REST_HEADER_TOKEN) String token,
                            @QueryParam (REST_KEYSPACES) String keyspaces) throws Exception {
        Date startTime;
        Date endTime;

        if (StringUtils.isBlank(daterange) || daterange.equalsIgnoreCase("default")) {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        } else {
            String[] restore = daterange.split(",");
            AbstractBackupPath path = pathProvider.get();
            startTime = path.getFormat().parse(restore[0]);
            endTime = path.getFormat().parse(restore[1]);
        }
        restore(token, region, startTime, endTime, keyspaces);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/incremental_restore")
    public Response restoreIncrementals() throws Exception {
        scheduler.addTask(IncrementalRestore.JOBNAME, IncrementalRestore.class, IncrementalRestore.getTimer());
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Path ("/list")
    public Response list(@QueryParam (REST_HEADER_RANGE) String daterange, @QueryParam (REST_HEADER_FILTER) String filter) throws Exception {
        Date startTime;
        Date endTime;

        if (StringUtils.isBlank(daterange) || daterange.equalsIgnoreCase("default")) {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        } else {
            String[] restore = daterange.split(",");
            AbstractBackupPath path = pathProvider.get();
            startTime = path.getFormat().parse(restore[0]);
            endTime = path.getFormat().parse(restore[1]);
        }
        Iterator<AbstractBackupPath> it = fs.list(backupConfiguration.getS3BucketName(), startTime, endTime);
        Map<String, Object> object = Maps.newHashMap();
        while (it.hasNext()) {
            AbstractBackupPath p = it.next();
            if (filter != null && BackupFileType.valueOf(filter) != p.getType()) {
                continue;
            }
            object.put(p.getRemotePath(), p.getFormat().format(p.getTime()));
        }
        return Response.ok(object, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/status")
    public Response status() throws Exception {
        int restoreTCount = restoreObj.getActiveCount();
        logger.debug("Thread counts for backup is: %d", restoreTCount);
        int backupTCount = fs.getActivecount();
        logger.debug("Thread counts for restore is: %d", backupTCount);
        Map<String, Object> object = Maps.newHashMap();
        object.put("Restore", new Integer(restoreTCount));
        object.put("Status", restoreObj.state().toString());
        object.put("Backup", new Integer(backupTCount));
        object.put("Status", snapshotBackup.state().toString());
        return Response.ok(object, MediaType.APPLICATION_JSON).build();
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
        String origRegion = amazonConfiguration.getRegionName();
        String origToken = priamServer.getInstanceIdentity().getInstance().getToken();
        if (StringUtils.isNotBlank(token)) {
            priamServer.getInstanceIdentity().getInstance().setToken(token);
        }

        if (backupConfiguration.isRestoreClosestToken()) {
            priamServer.getInstanceIdentity().getInstance().setToken(closestToken(priamServer.getInstanceIdentity().getInstance().getToken(), origRegion));
        }

        if (StringUtils.isNotBlank(region)) {
            amazonConfiguration.setRegionName(region);
            logger.info("Restoring from region " + region);
            priamServer.getInstanceIdentity().getInstance().setToken(closestToken(priamServer.getInstanceIdentity().getInstance().getToken(), region));
            logger.info("Restore will use token " + priamServer.getInstanceIdentity().getInstance().getToken());
        }

        setRestoreKeyspaces(keyspaces);

        try {
            restoreObj.restore(startTime, endTime);
        } finally {
            amazonConfiguration.setRegionName(origRegion);
            priamServer.getInstanceIdentity().getInstance().setToken(origToken);
        }
        tuneCassandra.updateYaml(false);
        SystemUtils.startCassandra(true, cassandraConfiguration, backupConfiguration, amazonConfiguration.getInstanceType());
    }

    /**
     * Find closest token in the specified region
     */
    private String closestToken(String token, String region) {
        List<PriamInstance> plist = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName());
        List<BigInteger> tokenList = Lists.newArrayList();
        for (PriamInstance ins : plist) {
            if (ins.getRegionName().equalsIgnoreCase(region)) {
                tokenList.add(new BigInteger(ins.getToken()));
            }
        }
        return TokenManager.findClosestToken(new BigInteger(token), tokenList).toString();
    }

    /*
     * TODO: decouple the servlet, config, and restorer. this should not rely on a side
     *       effect of a list mutation on the config object (treating it as global var).
     */
    private void setRestoreKeyspaces(String keyspaces) {
        List<String> list = backupConfiguration.getRestoreKeyspaces();
        list.clear();
        if (keyspaces != null) {
            logger.info("Restoring keyspaces: " + keyspaces);
            List<String> newKeyspaces = Lists.newArrayList(keyspaces.split(","));
            list.addAll(newKeyspaces);
        }
    }
}
