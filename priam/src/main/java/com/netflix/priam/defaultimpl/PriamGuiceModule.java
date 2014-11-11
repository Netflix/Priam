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
import com.netflix.priam.aws.S3CrossAccountFileSystem;
import com.netflix.priam.aws.S3EncryptedFileSystem;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.aws.auth.S3RoleAssumptionCredential;
import com.netflix.priam.backup.BackupFileSystemContext;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.cryptography.pgp.PgpCredential;
import com.netflix.priam.cryptography.pgp.PgpCryptography;
import com.netflix.priam.google.GcsCredential;
import com.netflix.priam.google.GoogleEncryptedFileSystem;
import com.netflix.priam.ICredentialGeneric;
import com.netflix.priam.google.GcsCredential;
import com.netflix.priam.cryptography.pgp.PgpCredential;
import com.netflix.priam.restore.EncryptedRestoreStrategy;
import com.netflix.priam.restore.IRestoreStrategy;
import com.netflix.priam.ICredential;


public class PriamGuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();

        bind(IBackupFileSystem.class).annotatedWith(Names.named("backup")).to(S3FileSystem.class);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("encryptedbackup")).to(S3EncryptedFileSystem.class);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("incr_restore")).to(S3FileSystem.class);
        bind(IBackupFileSystem.class).annotatedWith(Names.named("backup_status")).to(S3FileSystem.class);
        
        bind(S3CrossAccountFileSystem.class);
        bind(IFileSystemContext.class).annotatedWith(Names.named("backup")).to(BackupFileSystemContext.class);
        
        bind(IBackupFileSystem.class).annotatedWith(Names.named("gcsencryptedbackup")).to(GoogleEncryptedFileSystem.class);
        bind(IS3Credential.class).annotatedWith(Names.named("awsroleassumption")).to(S3RoleAssumptionCredential.class);
        bind(IFileCryptography.class).annotatedWith(Names.named("filecryptoalgorithm")).to(PgpCryptography.class);
        bind(ICredentialGeneric.class).annotatedWith(Names.named("gcscredential")).to(GcsCredential.class);
        bind(ICredentialGeneric.class).annotatedWith(Names.named("pgpcredential")).to(PgpCredential.class);
        bind(IRestoreStrategy.class).annotatedWith(Names.named("encryptedrestore")).to(EncryptedRestoreStrategy.class);
        bind(ICredential.class).to(ClearCredential.class);
    }
}