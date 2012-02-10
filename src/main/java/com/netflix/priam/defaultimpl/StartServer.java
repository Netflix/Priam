package com.netflix.priam.defaultimpl;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.netflix.priam.PriamServer;

/*
 * Initialize Priam server here
 *
 */
public class StartServer implements ServletContextListener
{
    @Override
    public void contextDestroyed(ServletContextEvent arg0)
    {
    }

    @Override
    public void contextInitialized(ServletContextEvent arg0)
    {
        try
        {
            PriamServer.instance.intialize(new PriamGuiceModule());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot init Guice module", e);
        }
    }

}
