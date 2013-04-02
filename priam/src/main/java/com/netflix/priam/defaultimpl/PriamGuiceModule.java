/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.defaultimpl;

import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.TokenManager;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.AWSMembership;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.SDBInstanceFactory;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.ThreadSleeper;

public class PriamGuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();
        bind(IConfiguration.class).to(PriamConfiguration.class).asEagerSingleton();
        bind(IPriamInstanceFactory.class).to(SDBInstanceFactory.class);
        bind(IMembership.class).to(AWSMembership.class);
        bind(ICredential.class).to(ClearCredential.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
        bind(Sleeper.class).to(ThreadSleeper.class);
        bind(ITokenManager.class).to(TokenManager.class);
    }
}
