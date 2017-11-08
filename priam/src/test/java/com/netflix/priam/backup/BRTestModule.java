/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.priam.backup;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.netflix.priam.*;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.aws.auth.S3RoleAssumptionCredential;
import com.netflix.priam.identity.FakeInstanceEnvIdentity;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.cryptography.pgp.PgpCryptography;
import com.netflix.priam.defaultimpl.FakeCassandraProcess;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceEnvIdentity;
import com.netflix.priam.identity.token.*;
import com.netflix.priam.merics.BackupMetricsMgr;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;
import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Arrays;
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
        bind(IBackupFileSystem.class).annotatedWith(Names.named("backup")).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("incr_restore")).to(FakeBackupFileSystem.class).in(Scopes.SINGLETON);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
        bind(Sleeper.class).to(FakeSleeper.class);
        bind(ITokenManager.class).to(TokenManager.class);
        //bind(ICassandraProcess.class).to(CassandraProcessManager.class);

        bind(IDeadTokenRetriever.class).to(DeadTokenRetriever.class);
        bind(IPreGeneratedTokenRetriever.class).to(PreGeneratedTokenRetriever.class);
        bind(INewTokenRetriever.class).to(NewTokenRetriever.class); //for backward compatibility, unit test always create new tokens        
        bind(IS3Credential.class).annotatedWith(Names.named("awss3roleassumption")).to(S3RoleAssumptionCredential.class);

        bind(IBackupFileSystem.class).annotatedWith(Names.named("encryptedbackup")).to(FakedS3EncryptedFileSystem.class);
        bind(IFileCryptography.class).annotatedWith(Names.named("filecryptoalgorithm")).to(PgpCryptography.class);
        bind(IIncrementalBackup.class).to(IncrementalBackup.class);
        bind(InstanceEnvIdentity.class).to(FakeInstanceEnvIdentity.class);
        bind(IBackupMetrics.class).to(BackupMetricsMgr.class);
        bind(ICassandraProcess.class).to(FakeCassandraProcess.class);
    }
}
