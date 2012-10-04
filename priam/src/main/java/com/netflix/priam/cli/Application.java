package com.netflix.priam.cli;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.IBackupFileSystem;

public class Application
{
    static private Injector injector;

    static Injector getInjector()
    {
        if (injector == null)
            injector = Guice.createInjector(new LightGuiceModule());
        return injector;
    }

    static void initialize()
    {
        IConfiguration conf = getInjector().getInstance(IConfiguration.class);
        conf.intialize();
    }

    static void shutdownAdditionalThreads()
    {
        IBackupFileSystem fs = getInjector().getInstance(IBackupFileSystem.class);
        fs.shutdown();
    }
}
