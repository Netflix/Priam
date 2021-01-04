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

package com.netflix.priam;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.priam.backup.FakeCredentials;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.NullBackupFileSystem;
import com.netflix.priam.config.FakeBackupRestoreConfig;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.identity.FakeMembership;
import com.netflix.priam.identity.FakePriamInstanceFactory;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.config.FakeInstanceInfo;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.Sleeper;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

@Ignore
public class TestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(IConfiguration.class).toInstance(new FakeConfiguration("fake-app"));
        bind(IBackupRestoreConfig.class).to(FakeBackupRestoreConfig.class);
        bind(InstanceInfo.class)
                .toInstance(new FakeInstanceInfo("fakeInstance1", "az1", "us-east-1"));

        bind(IPriamInstanceFactory.class).to(FakePriamInstanceFactory.class).in(Scopes.SINGLETON);
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(IMembership.class)
                .toInstance(
                        new FakeMembership(
                                ImmutableList.of(
                                        "fakeInstance1", "fakeInstance2", "fakeInstance3")));
        bind(ICredential.class).to(FakeCredentials.class).in(Scopes.SINGLETON);
        bind(IBackupFileSystem.class).to(NullBackupFileSystem.class);
        bind(Sleeper.class).to(FakeSleeper.class);
        bind(Registry.class).toInstance(new DefaultRegistry());
    }
}
