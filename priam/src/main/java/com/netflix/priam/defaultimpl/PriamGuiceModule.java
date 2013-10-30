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

import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.backup.IBackupFileSystem;

import com.netflix.priam.ICredential;


public class PriamGuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();

        bind(IBackupFileSystem.class).annotatedWith(Names.named("backup")).to(S3FileSystem.class);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("incr_restore")).to(S3FileSystem.class);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("backup_status")).to(S3FileSystem.class);
        bind(ICredential.class).to(ClearCredential.class);
    }
}
