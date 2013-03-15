package com.netflix.priam;

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
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;

@Ignore
public class TestModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(IConfiguration.class).toInstance(
                new FakeConfiguration(FakeConfiguration.FAKE_REGION, "fake-app", "az1", "fakeInstance1"));
        bind(IPriamInstanceFactory.class).to(FakePriamInstanceFactory.class);
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(IMembership.class).toInstance(new FakeMembership(
                ImmutableList.of("fakeInstance1", "fakeInstance2", "fakeInstance3")));
        bind(ICredential.class).to(FakeCredentials.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class).to(NullBackupFileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(Sleeper.class).to(FakeSleeper.class);
        bind(ITokenManager.class).to(TokenManager.class);
    }
}
