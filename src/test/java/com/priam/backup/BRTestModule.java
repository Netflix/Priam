package com.priam.backup;

import java.util.Arrays;

import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.priam.FakeConfiguration;
import com.priam.FakeMembership;
import com.priam.FakePriamInstanceFactory;
@Ignore
public class BRTestModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(IConfiguration.class).toInstance(new FakeConfiguration("fake-region", "fake-app", "fake-zone", "fakeInstance1"));
        bind(IPriamInstanceFactory.class).to(FakePriamInstanceFactory.class);
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(IMembership.class).toInstance(new FakeMembership(Arrays.asList("fakeInstance1")));
        bind(ICredential.class).to(FakeNullCredential.class).in(Scopes.SINGLETON);
        //bind(ICredential.class).to(FakeCredential.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
    }
}
