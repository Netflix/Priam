package com.priam.netflix;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Date;
import java.util.Set;

import com.netflix.aws.AWSManager;
import com.netflix.instance.identity.IdentityPersistenceException;
import com.netflix.instance.identity.InstanceData;
import com.netflix.instance.identity.aws.InstanceDataDAOSimpleDb;
import com.netflix.library.NFLibraryException;
import com.netflix.library.NFLibraryManager;
import com.netflix.platform.core.PlatformManager;
import com.priam.conf.JMXNodeTool;
import com.priam.utils.SystemUtils;

/**
* Usage within Netflix.
*
* java -client -Dnetflix.environment=test -Dnetflix.datacenter=cloud
* -Dnetflix.appinfo.name=s3cp
* -Dlog4j.logger.com.netflix.logging.dto.RealtimeTracerEntry=OFF
* -Dplatform.ListOfMandatoryComponentsToInit=
* -Dplatform.ListOfComponentsToInit=AWS
* -Dcom.netflix.akmsclient.akms.service.https.port=7102
* -Dcom.netflix.akmsclient
* .service.base.url=//ec2-50-17-124-101.compute-1.amazonaws
* .com:7102/cryptexservice/cryptex -Dnetflix.jmx.enabled=false
* -Dcom.netflix.akmsclient.keystore.search.path=/apps/tomcat -jar
* Validate-Priam.jar
*
*
* @author "Vijay Parthasarathy"
*
*/
public class ValidateInstanceID
{

    static final String APP_NAME = System.getenv("NETFLIX_APP");
    static final String REGION = System.getenv("EC2_REGION");
    static final String ZONE = System.getenv("EC2_AVAILABILITY_ZONE");
    static String INSTANCE_ID;
    static String PUBLIC_HOSTNAME;
    static String PUBLIC_IP;


    public static void main(String[] str) throws Exception
    {
        if (str.length > 0 && str[0].equals("-h"))
        {
            System.out.println("java -client -Dnetflix.environment=test \n" +
                    "-Dnetflix.datacenter=cloud \n" +
                    "-Dnetflix.appinfo.name=s3cp \n" +
                    "-Dlog4j.logger.com.netflix.logging.dto.RealtimeTracerEntry=OFF \n" +
                    "-Dplatform.ListOfMandatoryComponentsToInit= \n" +
                    "-Dplatform.ListOfComponentsToInit=AWS \n" +
                    "-Dcom.netflix.akmsclient.akms.service.https.port=7102 \n" +
                    "-Dcom.netflix.akmsclient.service.base.url=//ec2-50-17-124-101.compute-1.amazonaws .com:7102/cryptexservice/cryptex \n" +
                    "-Dnetflix.jmx.enabled=false \n" +
                    "-Dcom.netflix.akmsclient.keystore.search.path=/apps/tomcat -jar Validate-Priam.jar");
            System.exit(1);
        }
        
        init();
        Set<InstanceData> instances = InstanceDataDAOSimpleDb.getInstance().getAllIds(APP_NAME);

        InstanceData d = null;
        for (InstanceData data : instances)
        {
            if (!(data.getInstanceId().equalsIgnoreCase(INSTANCE_ID) || data.getInstanceId().equalsIgnoreCase(PUBLIC_IP)))
                continue;
            backup(data);
            int hash = SystemUtils.hash(REGION);
            d = new InstanceData(APP_NAME, hash + data.getId());
            d.setAvailabilityZone(ZONE);
            d.setElasticIP(PUBLIC_HOSTNAME + "," + PUBLIC_IP);
            d.setInstanceId(INSTANCE_ID);
            d.setLocation(REGION);
            if (str.length > 0 && str[0].equals("-sync"))
            {
                d.setPayload(new JMXNodeTool("localhost", 7501).getToken());
            }
            else
            {
                d.setPayload(data.getPayload());
            }
            d.setUpdateTimestamp(new Date(System.currentTimeMillis()));
            d.setVolumes(data.getVolumes());

            InstanceDataDAOSimpleDb.getInstance().deleteInstanceEntry(data);
            InstanceDataDAOSimpleDb.getInstance().registerInstance(d);

            System.exit(0);
        }
        System.exit(1);
    }

    protected static void init() throws NFLibraryException, IOException
    {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("netflix.environment", System.getenv("NETFLIX_ENVIRONMENT"));

        props.setProperty("netflix.datacenter", "cloud");

        props.setProperty("netflix.appinfo.name", "s3cp");

        props.setProperty("platform.ListOfMandatoryComponentsToInit", "");
        props.setProperty("netflix.jmx.enabled", "" + false);
        props.setProperty("com.netflix.akmsclient.keystore.search.path", "/apps/tomcat");

        props.setProperty("platform.ListOfComponentsToInit", "AWS");

        NFLibraryManager.initLibrary(PlatformManager.class, props, true, false);
        AWSManager.getInstance();
        
        INSTANCE_ID = getPublicURL("http://169.254.169.254/latest/meta-data/instance-id");
        PUBLIC_HOSTNAME = getPublicURL("http://169.254.169.254/latest/meta-data/public-hostname");
        PUBLIC_IP = getPublicURL("http://169.254.169.254/latest/meta-data/public-ipv4");

    }

    private static void backup(InstanceData data) throws IdentityPersistenceException
    {
        InstanceData d = new InstanceData(data.getApp() + "-bk", data.getId());
        d.setAvailabilityZone(data.getAvailabilityZone());
        d.setInstanceId(data.getInstanceId());
        d.setLocation(data.getLocation());
        d.setPayload(data.getPayload());
        d.setUpdateTimestamp(data.getUpdateTimestamp());
        d.setVolumes(data.getVolumes());
        InstanceDataDAOSimpleDb.getInstance().registerInstance(d);
    }

    private static String getPublicURL(String url) throws IOException
    {
        String publicIp = null;
        URL amazon_url = new URL(url);
        BufferedReader in = new BufferedReader(new InputStreamReader(amazon_url.openStream()));
        publicIp = in.readLine();
        return publicIp;
    }

}

