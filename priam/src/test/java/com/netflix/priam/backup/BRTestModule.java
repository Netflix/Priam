package com.netflix.priam.backup;

import java.util.Arrays;

import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.defaultimpl.CassandraProcessManager;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.TokenManager;
import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.FakeMembership;
import com.netflix.priam.FakePriamInstanceFactory;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.Sleeper;
@Ignore
public class BRTestModule extends AbstractModule
{
	
    @Override
    protected void configure()
    {
        bind(IConfiguration.class).toInstance(new FakeConfiguration(FakeConfiguration.FAKE_REGION, "fake-app", "az1", "fakeInstance1"));
        bind(IPriamInstanceFactory.class).to(FakePriamInstanceFactory.class);
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(IMembership.class).toInstance(new FakeMembership(Arrays.asList("fakeInstance1")));
        bind(ICredential.class).to(FakeNullCredential.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
        bind(Sleeper.class).to(FakeSleeper.class);
        bind(ITokenManager.class).to(TokenManager.class);
        bind(ICassandraProcess.class).to(CassandraProcessManager.class);
    }
}
