/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.merics;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.backup.IBackupMetrics;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
/**
 * Created by vinhn on 2/13/17.
 */
@Singleton
public class BackupMetrics implements IBackupMetrics {
    private final Counter validUploads, invalidUploads, validDownloads, invalidDownloads, backupUploadRateKbps, awsSlowDownException;

    @Inject
    public BackupMetrics(Registry registry){
        validDownloads = registry.counter("priam.download.valid");
        invalidDownloads = registry.counter("priam.download.invalid");
        validUploads = registry.counter("priam.upload.valid");
        invalidUploads = registry.counter("priam.upload.invalid");
        backupUploadRateKbps = registry.counter("priam.backup.upload.rate.kbps");
        awsSlowDownException = registry.counter("priam.aws.slowDown");
    }

    @Override
    public void incrementValidUploads() {
        this.validUploads.increment();
    }

    @Override
    public long getInvalidUploads() {
        return this.invalidUploads.count();
    }

    @Override
    public void incrementInvalidUploads() {
        this.invalidUploads.increment();
    }

    @Override
    public long getValidUploads() {
        return this.validUploads.count();
    }

    @Override
    public long getValidDownloads() {
        return this.validDownloads.count();
    }

    @Override
    public void incrementValidDownloads() {
        this.invalidDownloads.increment();
    }

    @Override
    public long getInvalidDownloads() {
        return this.invalidDownloads.count();
    }

    @Override
    public void incrementInvalidDownloads() {
        this.invalidDownloads.increment();
    }

    @Override
    public long getAwsSlowDownException() {
        return awsSlowDownException.count();
    }

    @Override
    public void incrementAwsSlowDownException(int awsSlowDown) {
        awsSlowDownException.increment(awsSlowDown);
    }

    @Override
    public long getBackupUploadRate() {
        return backupUploadRateKbps.count();
    }

    @Override
    public void incrementBackupUploadRate(long uploadRate) {
        backupUploadRateKbps.increment(uploadRate);
    }

}
