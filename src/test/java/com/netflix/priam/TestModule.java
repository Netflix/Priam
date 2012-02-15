package com.netflix.priam;

import java.util.Arrays;
import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.FakeCredentials;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;

@Ignore
public class TestModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(IConfiguration.class).toInstance(new FakeConfiguration("fake-region", "fake-app", "fake-zone", "fakeInstance1"));
        bind(IPriamInstanceFactory.class).to(FakePriamInstanceFactory.class);
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        String[] pinstances = { "fakeInstance1", "fakeInstance2", "fakeInstance3" };

        bind(IMembership.class).toInstance(new FakeMembership(Arrays.asList(pinstances)));
        bind(ICredential.class).to(FakeCredentials.class).in(Scopes.SINGLETON);
        // bind(IBackupRestoreFactory.class).to(FakeBackupRestoreFactory.class).in(Scopes.SINGLETON);
        // bind(JMXNodeTool.class).to(FakeNodeProbe.class);
        bind(IBackupFileSystem.class).to(FakeBackupFileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
    }

}
