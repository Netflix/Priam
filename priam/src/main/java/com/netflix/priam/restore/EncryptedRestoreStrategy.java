/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.restore;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.MetaData;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredentialGeneric;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A strategy to restore encrypted data from a primary AWS account
 */
@Singleton
public class EncryptedRestoreStrategy extends EncryptedRestoreBase {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedRestoreStrategy.class);
    public static final String JOBNAME = "CRYPTOGRAPHY_RESTORE_JOB";

    @Inject
    public EncryptedRestoreStrategy(
            final IConfiguration config,
            ICassandraProcess cassProcess,
            @Named("encryptedbackup") IBackupFileSystem fs,
            Sleeper sleeper,
            @Named("filecryptoalgorithm") IFileCryptography fileCryptography,
            @Named("pgpcredential") ICredentialGeneric credential,
            ICompression compress,
            Provider<AbstractBackupPath> pathProvider,
            InstanceIdentity id,
            RestoreTokenSelector tokenSelector,
            MetaData metaData,
            InstanceState instanceState,
            IPostRestoreHook postRestoreHook) {

        super(
                config,
                fs,
                JOBNAME,
                sleeper,
                cassProcess,
                pathProvider,
                id,
                tokenSelector,
                credential,
                fileCryptography,
                compress,
                metaData,
                instanceState,
                postRestoreHook);
    }

    /*
     * @return a timer used by the scheduler to determine when "this" should be run.
     */
    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }
}
