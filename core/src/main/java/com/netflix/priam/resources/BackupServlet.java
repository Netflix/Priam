package com.netflix.priam.resources;

import com.google.common.collect.ImmutableMap;
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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Path ("/v1/backup")
@Produces (MediaType.APPLICATION_JSON)
public class BackupServlet {
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    private static final Map<String, String> RESULT_OK = ImmutableMap.of("result", "ok");

    private PriamServer priamServer;
    private IBackupFileSystem fs;
    private Restore restoreObj;
    private Provider<AbstractBackupPath> pathProvider;
    private TuneCassandra tuneCassandra;
    private SnapshotBackup snapshotBackup;
    private IPriamInstanceRegistry instanceRegistry;
    private TokenManager tokenManager;
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
                         IPriamInstanceRegistry instanceRegistry,
                         TokenManager tokenManager) {
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
        this.tokenManager = tokenManager;
    }

    @GET
    @Path ("/do_snapshot")
    public Response backup() throws Exception {
        snapshotBackup.execute();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/restore")
    public Response restore(@QueryParam ("daterange") String daterange,
                            @QueryParam ("region") String region,
                            @QueryParam ("token") String token,
                            @QueryParam ("keyspaces") String keyspaces,
                            @QueryParam ("restoreprefix") String restorePrefix) throws Exception {

        Date startTime = new DateTime().minusDays(1).toDate();
        Date endTime = new DateTime().toDate();

        if (StringUtils.isNotBlank(daterange) && !daterange.equalsIgnoreCase("default")) {
            String[] restore = daterange.split(",");
            AbstractBackupPath path = pathProvider.get();
            startTime = path.getFormat().parse(restore[0]);
            endTime = path.getFormat().parse(restore[1]);
        }
        restore(token, region, startTime, endTime, keyspaces, restorePrefix);
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/incremental_restore")
    public Response restoreIncrementals() throws Exception {
        scheduler.addTask(IncrementalRestore.JOBNAME, IncrementalRestore.class, IncrementalRestore.getTimer());
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/list")
    public Response list(@QueryParam ("daterange") String daterange,
                         @QueryParam ("filter") String filter) throws Exception {

        Date startTime = new DateTime().minusDays(1).toDate();
        Date endTime = new DateTime().toDate();

        if (StringUtils.isNotBlank(daterange) && !daterange.equalsIgnoreCase("default")) {
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
        int backupTCount = fs.getActivecount();

        logger.debug("Thread counts for backup is: {}", restoreTCount);
        logger.debug("Thread counts for restore is: {}", backupTCount);

        Map<String, Object> object = Maps.newHashMap();
        object.put("Restore", ImmutableMap.of("Status", restoreObj.state().toString(), "Threads", restoreTCount));
        object.put("Backup", ImmutableMap.of("Status", snapshotBackup.state().toString(), "Threads", backupTCount));

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
    private void restore(String token, String region, Date startTime, Date endTime, String keyspaces, String restorePrefix) throws Exception {
        String origRegion = amazonConfiguration.getRegionName();
        String origToken = priamServer.getInstanceIdentity().getInstance().getToken();
        String origRestorePrefix = backupConfiguration.getRestorePrefix();

        try {
            if (StringUtils.isNotBlank(token)) {
                priamServer.getInstanceIdentity().getInstance().setToken(token);
            }
            if (backupConfiguration.isRestoreClosestToken()) {
                priamServer.getInstanceIdentity().getInstance().setToken(closestToken(priamServer.getInstanceIdentity().getInstance().getToken(), origRegion));
            }
            if (StringUtils.isNotBlank(restorePrefix)) {
                backupConfiguration.setRestorePrefix(restorePrefix);
            }
            if (StringUtils.isNotBlank(region)) {
                amazonConfiguration.setRegionName(region);
                logger.info("Restoring from region " + region);
                priamServer.getInstanceIdentity().getInstance().setToken(closestToken(priamServer.getInstanceIdentity().getInstance().getToken(), region));
                logger.info("Restore will use token " + priamServer.getInstanceIdentity().getInstance().getToken());
            }

            setRestoreKeyspaces(keyspaces);

            restoreObj.restore(startTime, endTime);

        } finally {
            amazonConfiguration.setRegionName(origRegion);
            priamServer.getInstanceIdentity().getInstance().setToken(origToken);
            backupConfiguration.setRestorePrefix(origRestorePrefix);
        }
        tuneCassandra.updateYaml(false);
        SystemUtils.startCassandra(true, cassandraConfiguration, backupConfiguration, amazonConfiguration.getInstanceType());
    }

    /**
     * Find closest token in the specified region
     */
    private String closestToken(String token, String region) {
        List<PriamInstance> plist = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName());
        List<String> tokenList = Lists.newArrayList();
        for (PriamInstance ins : plist) {
            if (ins.getRegionName().equalsIgnoreCase(region)) {
                tokenList.add(ins.getToken());
            }
        }
        return tokenManager.findClosestToken(token, tokenList);
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
