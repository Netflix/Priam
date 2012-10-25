package com.netflix.priam;

import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TokenManagerProvider;
import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.FakeCredentials;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.Sleeper;

@Ignore
public class TestModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(CassandraConfiguration.class).toInstance(new TestCassandraConfiguration("fake-app"));
        bind(AmazonConfiguration.class).toInstance(new TestAmazonConfiguration("fake-app", "fake-region", "az1", "fakeInstance1"));
        bind(BackupConfiguration.class).toInstance(new TestBackupConfiguration());
        bind(IPriamInstanceRegistry.class).to(FakePriamInstanceRegistry.class);
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(IMembership.class).toInstance(new FakeMembership(ImmutableList.of("fakeInstance1", "fakeInstance2", "fakeInstance3")));
        bind(ICredential.class).to(FakeCredentials.class).in(Scopes.SINGLETON);
        bind(TokenManager.class).toProvider(TokenManagerProvider.class);
        bind(IBackupFileSystem.class).to(NullBackupFileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(Sleeper.class).to(FakeSleeper.class);
    }
}
