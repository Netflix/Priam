package com.priam.backup;

import java.util.Arrays;

import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.priam.FakeCLConsumer;
import com.priam.FakeConfiguration;
import com.priam.FakeMembership;
import com.priam.FakePriamInstanceFactory;
import com.priam.aws.ICredential;
import com.priam.aws.S3BackupPath;
import com.priam.conf.IConfiguration;
import com.priam.identity.IMembership;
import com.priam.identity.IPriamInstanceFactory;
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
        bind(ICredential.class).to(FakeCredentials.class).in(Scopes.SINGLETON);
        // bind(JMXNodeTool.class).to(FakeNodeProbe.class);
        // bind(IBackupRestoreFactory.class).to(FakeBackupRestoreFactory.class).in(Scopes.SINGLETON);
        bind(Consumer.class).to(FakeCLConsumer.class).in(Scopes.SINGLETON);
        bindConstant().annotatedWith(Names.named("Mount EBS CL Backup")).to(new Boolean(false));
        // bind(FileUploader.class).toProvider(FakeFileUploaderProvider.class);
        bind(IBackupFileSystem.class).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
    }
}
