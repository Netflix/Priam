package com.netflix.priam.defaultimpl;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;

/*
 * Initialize Priam server here
 *
 */
public class StartServer implements ServletContextListener
{
    public Injector injector;

    @Override
    public void contextDestroyed(ServletContextEvent arg0)
    {
    }

    @Override
    public void contextInitialized(ServletContextEvent arg0)
    {
        try
        {
            injector = Guice.createInjector(new PriamGuiceModule());
            injector.getInstance(IConfiguration.class).intialize();
            injector.getInstance(PriamServer.class).intialize();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot init Guice module", e);
        }
    }

}
