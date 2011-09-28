package com.priam.servlets;

import java.util.Date;
import java.util.Iterator;

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
import com.priam.backup.AbstractBackupPath;
import com.priam.backup.IBackupFileSystem;
import com.priam.backup.Restore;
import com.priam.backup.SnapshotBackup;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.conf.PriamServer;
import com.priam.utils.SystemUtils;

@Path("/backup")
@Produces({ "application/text" })
public class BackupServlet
{
    private static final Logger logger = LoggerFactory.getLogger(BackupServlet.class);

    public static final String REST_HEADER_KEY = "type";
    public static final String REST_HEADER_VALUE = "ext";
    private static final String REST_HEADER_FILTER = "filter";

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response restore(@QueryParam(REST_HEADER_KEY) String type, 
            @QueryParam(REST_HEADER_VALUE) String value,
            @QueryParam(REST_HEADER_FILTER) String filter) throws Exception
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
            SystemUtils.stopCassandra();
            PriamServer.instance.injector.getInstance(Restore.class).restore(startTime, endTime);
            SystemUtils.startCassandra(true, PriamServer.instance.config);
            return Response.ok("Backup completed").build();
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
            return Response.ok("Restore: " + restoreTCount
                    + "\nStatus: " + res.state()
                    + "\nBackup: " + backupTCount
                    + "\nStatus: " + sb.state()).build();
        }

        logger.error(String.format("Couldnt serve the URL with parameters: type=%s and ext=%s", type, value));
        return Response.status(404).build();
    }
}
