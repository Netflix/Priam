package com.netflix.priam.backup;

import java.util.Arrays;

import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.defaultimpl.CassandraProcessManager;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.merics.NoOpMetricPublisher;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.notification.INotificationService;
import com.netflix.priam.notification.NoOpNotificationService;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.TokenManager;

import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.FakeMembership;
import com.netflix.priam.FakePriamInstanceFactory;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.aws.auth.S3RoleAssumptionCredential;
import com.netflix.priam.backup.identity.FakeInstanceEnvIdentity;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.cryptography.pgp.PgpCryptography;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceEnvIdentity;
import com.netflix.priam.identity.token.DeadTokenRetriever;
import com.netflix.priam.identity.token.IDeadTokenRetriever;
import com.netflix.priam.identity.token.INewTokenRetriever;
import com.netflix.priam.identity.token.IPreGeneratedTokenRetriever;
import com.netflix.priam.identity.token.NewTokenRetriever;
import com.netflix.priam.identity.token.PreGeneratedTokenRetriever;
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
//        bind(IBackupFileSystem.class).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("backup")).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("incr_restore")).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
        bind(Sleeper.class).to(FakeSleeper.class);
        bind(ITokenManager.class).to(TokenManager.class);
        bind(ICassandraProcess.class).to(CassandraProcessManager.class);

        bind(IDeadTokenRetriever.class).to(DeadTokenRetriever.class);
        bind(IPreGeneratedTokenRetriever.class).to(PreGeneratedTokenRetriever.class);
        bind(INewTokenRetriever.class).to(NewTokenRetriever.class); //for backward compatibility, unit test always create new tokens        
        bind(IS3Credential.class).annotatedWith(Names.named("awss3roleassumption")).to(S3RoleAssumptionCredential.class);

        bind(IFileSystemContext.class).annotatedWith(Names.named("backup")).to(BackupFileSystemContext.class);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("encryptedbackup")).to(FakedS3EncryptedFileSystem.class);
        bind(IFileCryptography.class).annotatedWith(Names.named("filecryptoalgorithm")).to(PgpCryptography.class);
        bind(IIncrementalBackup.class).to(IncrementalBackup.class);
        bind(InstanceEnvIdentity.class).to(FakeInstanceEnvIdentity.class);
        bind(IMetricPublisher.class).annotatedWith(Names.named("defaultmetricpublisher")).to(NoOpMetricPublisher.class);
        bind(INotificationService.class).annotatedWith(Names.named("defaultnotificationservice")).to(NoOpNotificationService.class);
    }
}
