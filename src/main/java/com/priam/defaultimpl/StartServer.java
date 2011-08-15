package com.priam.defaultimpl;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.priam.conf.PriamServer;

/**
 * Startup Servelet which will be used to intialize services and register jobs.
 * 
 * @author "Vijay Parthasarathy"
 */
public class StartServer implements ServletContextListener
{
    private static final String APP_NAME = "Priam";
    private static final Logger logger = LoggerFactory.getLogger(StartServer.class);

    @Override
    public void contextDestroyed(ServletContextEvent arg0)
    {
    }

    @Override
    public void contextInitialized(ServletContextEvent arg0)
    {
        logger.info("Using App name as: " + APP_NAME);
        // intialize priam server.
        try
        {
            PriamServer.instance.intialize(new GuiceModule());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
