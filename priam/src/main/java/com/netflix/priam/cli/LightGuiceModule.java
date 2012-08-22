package com.netflix.priam.cli;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.defaultimpl.PriamConfiguration;
import com.netflix.priam.defaultimpl.ClearCredential;
import com.netflix.priam.aws.AWSMembership;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.SDBInstanceFactory;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.ThreadSleeper;

class LightGuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(IConfiguration.class).to(PriamConfiguration.class).asEagerSingleton();
        bind(ICredential.class).to(ClearCredential.class);
        bind(IPriamInstanceFactory.class).to(SDBInstanceFactory.class);
        bind(IMembership.class).to(AWSMembership.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
        bind(Sleeper.class).to(ThreadSleeper.class);
    }
}

class Application
{
    static final Injector injector = Guice.createInjector(new LightGuiceModule());

    static void initialize()
    {
        IConfiguration conf = injector.getInstance(IConfiguration.class);
        conf.intialize();
    }

    static void shutdownAdditionalThreads()
    {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        fs.shutdown();
    }
}
