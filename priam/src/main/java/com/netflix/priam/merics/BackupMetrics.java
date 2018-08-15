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
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;

/**
 * Created by vinhn on 2/13/17.
 */
@Singleton
public class BackupMetrics {
    /**
     * Distribution summary will provide the metric like count (how many uploads were made), max no. of bytes uploaded and total amount of bytes uploaded.
     */
    private final DistributionSummary uploadRate;
    private final Counter validUploads, invalidUploads, validDownloads, invalidDownloads, awsSlowDownException, snsNotificationSuccess, snsNotificationFailure;

    @Inject
    public BackupMetrics(Registry registry) {
        validDownloads = registry.counter(Metrics.METRIC_PREFIX + "download.valid");
        invalidDownloads = registry.counter(Metrics.METRIC_PREFIX + "download.invalid");
        validUploads = registry.counter(Metrics.METRIC_PREFIX + "upload.valid");
        invalidUploads = registry.counter(Metrics.METRIC_PREFIX + "upload.invalid");
        uploadRate = registry.distributionSummary(Metrics.METRIC_PREFIX + "upload.rate");
        awsSlowDownException = registry.counter(Metrics.METRIC_PREFIX + "aws.slowDown");
        snsNotificationSuccess = registry.counter(Metrics.METRIC_PREFIX + "sns.notification.success");
        snsNotificationFailure = registry.counter(Metrics.METRIC_PREFIX + "sns.notification.failure");
    }

    public void incrementValidUploads() {
        this.validUploads.increment();
    }

    public void incrementInvalidUploads() {
        this.invalidUploads.increment();
    }

    public long getValidUploads() {
        return this.validUploads.count();
    }

    public long getValidDownloads() {
        return this.validDownloads.count();
    }

    public void incrementValidDownloads() {
        this.invalidDownloads.increment();
    }


    public void incrementInvalidDownloads() {
        this.invalidDownloads.increment();
    }

    public long getAwsSlowDownException() {
        return awsSlowDownException.count();
    }

    public void incrementAwsSlowDownException(int awsSlowDown) {
        awsSlowDownException.increment(awsSlowDown);
    }

    public void incrementSnsNotificationSuccess() {
        snsNotificationSuccess.increment();
    }

    public void incrementSnsNotificationFailure() {
        snsNotificationFailure.increment();
    }

    public void recordUploadRate(long sizeInBytes) {
        uploadRate.record(sizeInBytes);
    }

}
