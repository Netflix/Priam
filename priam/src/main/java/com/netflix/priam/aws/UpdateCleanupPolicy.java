/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.aws;

import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/** Updates the cleanup policy for the bucket */
@Singleton
public class UpdateCleanupPolicy extends Task {
    public static final String JOBNAME = "UpdateCleanupPolicy";
    private final IBackupFileSystem fs;

    @Inject
    public UpdateCleanupPolicy(IConfiguration config, @Named("backup") IBackupFileSystem fs) {
        super(config);
        this.fs = fs;
    }

    @Override
    public void execute() throws Exception {
        // Set cleanup policy of retention is specified
        new RetryableCallable<Void>() {
            @Override
            public Void retriableCall() throws Exception {
                fs.cleanup();
                return null;
            }
        }.call();
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }
}
