package com.priam.servlets;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.priam.backup.AbstractBackupPath;
import com.priam.backup.IBackupFileSystem;
import com.priam.backup.Restore;
import com.priam.backup.SnapshotBackup;
import com.priam.conf.JMXNodeTool;
import com.priam.conf.PriamServer;
import com.priam.identity.DoubleRing;
import com.priam.identity.IMembership;
import com.priam.identity.IPriamInstanceFactory;
import com.priam.identity.InstanceIdentity;
import com.priam.identity.PriamInstance;
import com.priam.utils.RetryableCallable;
import com.priam.utils.SystemUtils;

/**
 * This servlet will provide the configuration API service as and when cassandra
 * requests for it.
 * 
 * @author "Vijay Parthasarathy"
 */
@Path("/cassandra_config/{clusterName}")
@Produces({ "application/text" })
public class CConfig
{
    private static final Logger logger = LoggerFactory.getLogger(CConfig.class);
    private static final String REST_HEADER_TYPE = "type";

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getConfigGET(@QueryParam(REST_HEADER_TYPE) String request_type)
    {
        return getTypedResponse(request_type);
    }

    private Response getTypedResponse(String request_type)
    {
        try {
            if (request_type.equalsIgnoreCase("GET_SEEDS")) {
                if (PriamServer.instance.id.getSeeds() != null && PriamServer.instance.id.getSeeds().size() != 0)
                    return Response.ok(StringUtils.join(PriamServer.instance.id.getSeeds(), ',')).build();
                logger.error("Cannot find the Seeds " + PriamServer.instance.id.getSeeds());
            }
            else if (request_type.equalsIgnoreCase("GET_TOKEN")) {
                if (PriamServer.instance.id.getInstance().getPayload() != null)
                    return Response.ok(PriamServer.instance.id.getInstance().getPayload()).build();
                logger.error("Cannot find the token...:" + PriamServer.instance.id.getInstance().getPayload());
            }
            else if (request_type.equalsIgnoreCase("IS_REPLACE_TOKEN")) {
                return Response.ok("" + PriamServer.instance.id.isReplace).build();
            }
            else if (request_type.equalsIgnoreCase("DOUBLE_RING")) {
                DoubleRing ring = PriamServer.instance.injector.getInstance(DoubleRing.class);
                try {
                    ring.backup();
                    ring.doubleSlots();
                }
                catch (Throwable th) {
                    logger.error("Error in doubling the ring...", th);
                    ring.restore();
                    // rethrow
                    throw new RuntimeException(th);
                }
                return Response.status(200).build();
            }
            else if (request_type.equalsIgnoreCase("DECOMISSION")) {
                setOutOfService();
                IMembership membership = PriamServer.instance.injector.getInstance(IMembership.class);
                return Response.ok(String.format("%s is set out of service and ASG size increased to %d", PriamServer.instance.id.getInstance().getInstanceId(), membership.getRacMembershipSize()))
                        .build();
            }

        }
        catch (Exception e) {
            logger.error("Error while executing the servlet... : ", e);
        }
        logger.error(String.format("Couldnt serve the URL with parameters: type=%s", request_type));
        return Response.status(404).build();
    }

    private void setOutOfService() throws Exception
    {
        // Take it out from discovery
        logger.info(getDiscoveryURI());

        new RetryableCallable<Void>()
        {
            @Override
            public Void retriableCall() throws Exception
            {
                URL url = new URL(getDiscoveryURI());
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setDoOutput(false);
                connection.setRequestMethod("PUT");
                int code = connection.getResponseCode();
                if (code != 200) {
                    Thread.sleep(1000);
                    String msg = String.format("Unable to update discovery. Error code=%d %s", code, connection.getResponseMessage());
                    logger.error(msg);
                    throw new Exception(msg);
                }
                return null;
            }
        }.call();
        Thread.sleep(60000);
        logger.info("Removed from discovery");
        // Mark the instance as dead (and backup)
        IPriamInstanceFactory factory = PriamServer.instance.injector.getInstance(IPriamInstanceFactory.class);
        PriamInstance ins = PriamServer.instance.id.getInstance();
        factory.create(ins.getApp() + "-dead", ins.getId(), ins.getInstanceId(), ins.getHostName(), ins.getHostIP(), ins.getRac(), ins.getVolumes(), ins.getPayload());
        ins.setOutOfService(true);

        // Update with new slot
        factory.delete(ins);
        factory.create(ins.getApp(), ins.getId(), "new_slot", "new_host", "new_IP", ins.getRac(), null, ins.getPayload());
        logger.info("Marked instanced as out of service in db");

        // Kill cassandra
        SystemUtils.stopCassandra();

        // Increase the size of ASG by 1
        PriamServer.instance.injector.getInstance(IMembership.class).expandRacMembership(1);
    }

    private String getDiscoveryURI()
    {
        PriamInstance ins = PriamServer.instance.id.getInstance();
        if (System.getenv("NETFLIX_ENVIRONMENT").equalsIgnoreCase("test"))
            return String.format("http://%s.discovery.cloudqa.netflix.net:7001/discovery/v2/apps/%s/%s/status?value=OUT_OF_SERVICE", System.getenv("EC2_AVAILABILITY_ZONE"),
                    System.getenv("NETFLIX_APP"), ins.getInstanceId());
        else
            return String.format("http://%s.discovery.cloud.netflix.net:7001/discovery/v2/apps/%s/%s/status?value=OUT_OF_SERVICE", System.getenv("EC2_AVAILABILITY_ZONE"),
                    System.getenv("NETFLIX_APP"), ins.getInstanceId());
    }
}
