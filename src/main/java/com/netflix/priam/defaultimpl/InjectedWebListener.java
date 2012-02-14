package com.netflix.priam.defaultimpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContextEvent;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;

public class InjectedWebListener extends GuiceServletContextListener
{

    private Injector injector;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent)
    {
        try
        {
            injector.getInstance(IConfiguration.class).intialize();
            injector.getInstance(PriamServer.class).intialize();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected Injector getInjector()
    {
        List<Module> moduleList = Lists.newArrayList();
        moduleList.add(new JaxServletModule());
        moduleList.add(new PriamGuiceModule());
        injector = Guice.createInjector(moduleList);
        return injector;
    }

    public static class JaxServletModule extends ServletModule
    {
        @Override
        protected void configureServlets()
        {
            Map<String, String> params = new HashMap<String, String>();
            params.put(PackagesResourceConfig.PROPERTY_PACKAGES, "unbound");
            params.put("com.sun.jersey.config.property.packages", "com.netflix.priam.resources");
            params.put(ServletContainer.PROPERTY_FILTER_CONTEXT_PATH, "/REST");
            serve("/REST/*").with(GuiceContainer.class, params);
        }
    }

}