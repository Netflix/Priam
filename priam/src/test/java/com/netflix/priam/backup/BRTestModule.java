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
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.aws.auth.S3RoleAssumptionCredential;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.backupv2.MetaV1Proxy;
import com.netflix.priam.backupv2.MetaV2Proxy;
import com.netflix.priam.config.FakeBackupRestoreConfig;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.cryptography.pgp.PgpCryptography;
import com.netflix.priam.defaultimpl.FakeCassandraProcess;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.identity.FakeMembership;
import com.netflix.priam.identity.FakePriamInstanceFactory;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.config.FakeInstanceInfo;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.restore.IPostRestoreHook;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.Sleeper;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

@Ignore
public class BRTestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(IConfiguration.class).toInstance(new FakeConfiguration("fake-app"));
        bind(IBackupRestoreConfig.class).to(FakeBackupRestoreConfig.class);
        bind(InstanceInfo.class)
                .toInstance(new FakeInstanceInfo("fakeInstance1", "az1", "us-east-1"));

        bind(IPriamInstanceFactory.class).to(FakePriamInstanceFactory.class);
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(IMembership.class)
                .toInstance(new FakeMembership(Collections.singletonList("fakeInstance1")));
        bind(ICredential.class).to(FakeNullCredential.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class)
                .annotatedWith(Names.named("backup"))
                .to(FakeBackupFileSystem.class)
                .in(Scopes.SINGLETON);
        bind(Sleeper.class).to(FakeSleeper.class);

        bind(IS3Credential.class)
                .annotatedWith(Names.named("awss3roleassumption"))
                .to(S3RoleAssumptionCredential.class);

        bind(IBackupFileSystem.class)
                .annotatedWith(Names.named("encryptedbackup"))
                .to(NullBackupFileSystem.class);
        bind(IFileCryptography.class)
                .annotatedWith(Names.named("filecryptoalgorithm"))
                .to(PgpCryptography.class);
        bind(ICassandraProcess.class).to(FakeCassandraProcess.class);
        bind(IPostRestoreHook.class).to(FakePostRestoreHook.class);
        bind(Registry.class).toInstance(new DefaultRegistry());
        bind(IMetaProxy.class).annotatedWith(Names.named("v1")).to(MetaV1Proxy.class);
        bind(IMetaProxy.class).annotatedWith(Names.named("v2")).to(MetaV2Proxy.class);
        bind(DynamicRateLimiter.class).to(FakeDynamicRateLimiter.class);
        bind(Clock.class).toInstance(Clock.fixed(Instant.EPOCH, ZoneId.systemDefault()));
    }
}
