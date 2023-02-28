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
package com.netflix.priam.merics;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Registry;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Created by vinhn on 2/13/17. */
@Singleton
public class BackupMetrics {
    private final Registry registry;
    /**
     * Distribution summary will provide the metric like count (how many uploads were made), max no.
     * of bytes uploaded and total amount of bytes uploaded.
     */
    private final DistributionSummary uploadRate, downloadRate;

    private final Counter validUploads,
            validDownloads,
            invalidUploads,
            invalidDownloads,
            snsNotificationSuccess,
            snsNotificationFailure,
            forgottenFiles,
            backupVerificationFailure;
    public static final String uploadQueueSize = Metrics.METRIC_PREFIX + "upload.queue.size";
    public static final String downloadQueueSize = Metrics.METRIC_PREFIX + "download.queue.size";

    @Inject
    public BackupMetrics(Registry registry) {
        this.registry = registry;
        validDownloads = registry.counter(Metrics.METRIC_PREFIX + "download.valid");
        invalidDownloads = registry.counter(Metrics.METRIC_PREFIX + "download.invalid");
        validUploads = registry.counter(Metrics.METRIC_PREFIX + "upload.valid");
        invalidUploads = registry.counter(Metrics.METRIC_PREFIX + "upload.invalid");
        uploadRate = registry.distributionSummary(Metrics.METRIC_PREFIX + "upload.rate");
        downloadRate = registry.distributionSummary(Metrics.METRIC_PREFIX + "download.rate");
        snsNotificationSuccess =
                registry.counter(Metrics.METRIC_PREFIX + "sns.notification.success");
        snsNotificationFailure =
                registry.counter(Metrics.METRIC_PREFIX + "sns.notification.failure");
        forgottenFiles = registry.counter(Metrics.METRIC_PREFIX + "forgotten.files");
        backupVerificationFailure =
                registry.counter(Metrics.METRIC_PREFIX + "backup.verification.failure");
    }

    public DistributionSummary getUploadRate() {
        return uploadRate;
    }

    public Counter getInvalidUploads() {
        return invalidUploads;
    }

    public Counter getInvalidDownloads() {
        return invalidDownloads;
    }

    public Counter getSnsNotificationSuccess() {
        return snsNotificationSuccess;
    }

    public Counter getSnsNotificationFailure() {
        return snsNotificationFailure;
    }

    public void incrementInvalidUploads() {
        this.invalidUploads.increment();
    }

    public void incrementInvalidDownloads() {
        this.invalidDownloads.increment();
    }

    public void incrementSnsNotificationSuccess() {
        snsNotificationSuccess.increment();
    }

    public void incrementSnsNotificationFailure() {
        snsNotificationFailure.increment();
    }

    public void incrementBackupVerificationFailure() {
        backupVerificationFailure.increment();
    }

    public void recordUploadRate(long sizeInBytes) {
        uploadRate.record(sizeInBytes);
    }

    public void incrementForgottenFiles(long forgottenFilesVal) {
        forgottenFiles.increment(forgottenFilesVal);
    }

    public void recordDownloadRate(long sizeInBytes) {
        downloadRate.record(sizeInBytes);
    }

    public DistributionSummary getDownloadRate() {
        return downloadRate;
    }

    public Counter getValidUploads() {
        return validUploads;
    }

    public Counter getValidDownloads() {
        return validDownloads;
    }

    public void incrementValidUploads() {
        this.validUploads.increment();
    }

    public void incrementValidDownloads() {
        this.validDownloads.increment();
    }

    public Registry getRegistry() {
        return registry;
    }
}
