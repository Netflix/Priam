package com.netflix.priam.servlets;

import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.json.JSONObject;
import com.google.common.collect.Lists;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.PriamServer;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TuneCassandra;

@Path("/backup")
@Produces({ "application/text" })
public class BackupServlet
{
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    public static final String REST_HEADER_KEY = "type";
    public static final String REST_HEADER_VALUE = "ext";
    private static final String REST_HEADER_FILTER = "filter";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_HEADER_REGION = "region";
    private static final String REST_KEYSPACES = "keyspaces";

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response restore(@QueryParam(REST_HEADER_KEY) String type, @QueryParam(REST_HEADER_VALUE) String value, @QueryParam(REST_HEADER_FILTER) String filter,
            @QueryParam(REST_HEADER_REGION) String region, @QueryParam(REST_HEADER_TOKEN) String token, @QueryParam(REST_KEYSPACES) String keyspaces) throws Exception
    {
        Date startTime;
        Date endTime;
        AbstractBackupPath path = PriamServer.instance.injector.getInstance(AbstractBackupPath.class);
        String[] restore = value.split(",");
        if (value.equalsIgnoreCase("default"))
        {
            startTime = new DateTime().minusDays(1).toDate();
            endTime = new DateTime().toDate();
        }
        else
        {
            startTime = path.getFormat().parse(restore[0]);
            endTime = path.getFormat().parse(restore[1]);
        }
        if (type.equalsIgnoreCase("restore"))
        {
            //SystemUtils.stopCassandra();
            String origRegion = PriamServer.instance.config.getDC();
            String origToken = PriamServer.instance.id.getInstance().getPayload();
            if (token != null && token != "")
                PriamServer.instance.id.getInstance().setPayload(token);

            if (region != null && region != "")
            {
                PriamServer.instance.config.setDC(region);
                logger.info("Restoring from region " + region);
                PriamServer.instance.id.getInstance().setPayload(closestToken(PriamServer.instance.id.getInstance().getPayload(), region));
                logger.info("Restore will use token " + PriamServer.instance.id.getInstance().getPayload());
            }

            setRestoreKeyspaces(keyspaces);

            try 
            {
                PriamServer.instance.injector.getInstance(Restore.class).restore(startTime, endTime);
            }
            finally
            {
                PriamServer.instance.config.setDC(origRegion);
                PriamServer.instance.id.getInstance().setPayload(origToken);
            }
            PriamServer.instance.injector.getInstance(TuneCassandra.class).updateYaml(false);
            SystemUtils.startCassandra(true, PriamServer.instance.config);
            return Response.ok("Restore completed").build();

        }
        else if (type.equalsIgnoreCase("list"))
        {
            IBackupFileSystem fs = PriamServer.instance.injector.getInstance(IBackupFileSystem.class);
            Iterator<AbstractBackupPath> it = fs.list(PriamServer.instance.config.getBackupPrefix(), startTime, endTime);
            JSONObject object = new JSONObject();
            while (it.hasNext())
            {
                AbstractBackupPath p = it.next();
                if (filter != null && BackupFileType.valueOf(filter) != p.type)
                    continue;
                object.put(p.getRemotePath(), p.getFormat().format(p.time));
            }
            return Response.ok(object.toString()).build();
        }
        else if (type.equalsIgnoreCase("do_snapshot"))
        {
            PriamServer.instance.injector.getInstance(SnapshotBackup.class).execute();
            return Response.status(200).build();
        }
        else if (type.equalsIgnoreCase("status"))
        {
            Restore res = PriamServer.instance.injector.getInstance(Restore.class);
            int restoreTCount = res.getActiveCount();
            logger.debug("Thread counts for backup is: %d", restoreTCount);
            int backupTCount = PriamServer.instance.injector.getInstance(IBackupFileSystem.class).getActivecount();
            logger.debug("Thread counts for restore is: %d", backupTCount);
            SnapshotBackup sb = PriamServer.instance.injector.getInstance(SnapshotBackup.class);
            return Response.ok("Restore: " + restoreTCount + "\nStatus: " + res.state() + "\nBackup: " + backupTCount + "\nStatus: " + sb.state()).build();
        }

        logger.error(String.format("Couldnt serve the URL with parameters: type=%s and ext=%s", type, value));
        return Response.status(404).build();
    }

    public String closestToken(String token, String region)
    {
        IPriamInstanceFactory factory = PriamServer.instance.injector.getInstance(IPriamInstanceFactory.class);
        List<PriamInstance> plist = factory.getAllIds(PriamServer.instance.config.getAppName());
        List<BigInteger> tokenList = Lists.newArrayList();
        for (PriamInstance ins : plist)
        {
            if (ins.location.equalsIgnoreCase(region))
                tokenList.add(new BigInteger(ins.getPayload()));
        }
        return TokenManager.findClosestToken(new BigInteger(token), tokenList).toString();
    }
    
    private void setRestoreKeyspaces(String keyspaces){
        List<String> list = PriamServer.instance.config.getRestoreKeySpaces();
        list.clear();
        if( keyspaces != null){
            logger.info("Restoring keyspaces: " + keyspaces);
            List<String> newKeyspaces = Lists.newArrayList(keyspaces.split(","));
            list.addAll(newKeyspaces);
        }
    }
}
