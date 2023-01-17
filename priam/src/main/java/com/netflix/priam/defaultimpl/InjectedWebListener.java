/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.defaultimpl;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.netflix.priam.PriamServer;
import com.netflix.priam.config.IConfiguration;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletContextEvent;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InjectedWebListener extends GuiceServletContextListener {
    protected static final Logger logger = LoggerFactory.getLogger(InjectedWebListener.class);
    private Injector injector;

    @Override
    protected Injector getInjector() {
        List<Module> moduleList = Lists.newArrayList();
        moduleList.add(new JaxServletModule());
        moduleList.add(new PriamGuiceModule());
        injector = Guice.createInjector(moduleList);
        try {
            injector.getInstance(IConfiguration.class).initialize();
            injector.getInstance(PriamServer.class).scheduleService();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return injector;
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        try {
            for (Scheduler scheduler :
                    injector.getInstance(SchedulerFactory.class).getAllSchedulers()) {
                scheduler.shutdown();
            }
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
        super.contextDestroyed(servletContextEvent);
    }

    public static class JaxServletModule extends ServletModule {
        @Override
        protected void configureServlets() {
            Map<String, String> params = new HashMap<>();
            params.put(PackagesResourceConfig.PROPERTY_PACKAGES, "unbound");
            params.put("com.sun.jersey.config.property.packages", "com.netflix.priam.resources");
            params.put(ServletContainer.PROPERTY_FILTER_CONTEXT_PATH, "/REST");
            serve("/REST/*").with(GuiceContainer.class, params);
        }
    }
}
