package com.priam.netflix;

import java.io.File;
import java.util.Properties;

import javax.servlet.ServletContextEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.NetflixConfiguration;
import com.netflix.server.base.BaseServer;
import com.priam.conf.PriamServer;

/**
 * Startup servlet which will be used to intialize services and register jobs.
 * 
 * @author "Vijay Parthasarathy"
 */
public class StartServer extends BaseServer
{
    private static final String APP_NAME = "Priam";
    private static final Logger logger = LoggerFactory.getLogger(HealthCheckServlet.class);
    private static String bootPropFile = "/etc/Priam.properties";
    /**
     * This is hack to get the netflix app info name as application name of the
     * ASG
     */
    static
    {
        // This make sure... that we always talk to USE
        System.setProperty("netflix.platform.SimpleDB.EndPoint", "sdb.amazonaws.com");
        System.setProperty("netflix.appinfo.name", System.getenv("NETFLIX_APP"));
        System.setProperty("netflix.appinfo.healthCheckUrl", "http://${netflix.appinfo.hostname}:8080/" + APP_NAME + "/healthcheck");
        // Unfortunately hard coded as properties is getting loaded but does not
        // reflect. **FIX SOON**
        System.setProperty("netflix.appinfo.port", "7102");
    }

    /**
     * Creates a new StartServer object.
     */
    public StartServer()
    {
        super(APP_NAME, APP_NAME, null);
    }

    @Override
    protected void initialize(Properties props) throws Exception
    {
        logger.info("Using App name as: " + APP_NAME);
        logger.info("Initializing epic monitor object");
        // intialize priam server.
        PriamServer.instance.intialize(new GuiceModule(getBootSource()));
    }

    @Override
    public void contextInitialized(ServletContextEvent sce)
    {
        try
        {
            if (new File(bootPropFile).exists())
                NetflixConfiguration.loadApplicationOverridePropertiesFile(bootPropFile);
        }
        catch (Exception e)
        {
            // log and ignore
            logger.error(e.getMessage(), e);
        }
        super.contextInitialized(sce);
    }

    private GuiceModule.BootSource getBootSource()
    {
        try
        {
            NetflixConfiguration conf = NetflixConfiguration.getInstance();
            logger.info("Is boot cluster: " + NetflixConfiguration.getInstance().getBoolean("Priam.localbootstrap.enable", false));
            logger.info("Boot source: " + GuiceModule.BootSource.valueOf(conf.getString("Priam.bootsource", "CASSANDRA")));
            // Backward compatible
            if (conf.getBoolean("Priam.localbootstrap.enable", false))
                return GuiceModule.BootSource.LOCAL;
            return GuiceModule.BootSource.valueOf(conf.getString("Priam.bootsource", "CASSANDRA"));
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        }
        return GuiceModule.BootSource.CASSANDRA;
    }
}
