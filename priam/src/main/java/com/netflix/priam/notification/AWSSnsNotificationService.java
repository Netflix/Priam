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
package com.netflix.priam.notification;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.netflix.priam.aws.IAMCredential;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A single, persisted, connection to Amazon SNS.
 */
@Singleton
public class AWSSnsNotificationService implements INotificationService {
    private static final Logger logger = LoggerFactory.getLogger(AWSSnsNotificationService.class);

    private final IConfiguration configuration;
    private final AmazonSNS snsClient;
    private final BackupMetrics backupMetrics;

    @Inject
    public AWSSnsNotificationService(
            IConfiguration config,
            IAMCredential iamCredential,
            BackupMetrics backupMetrics,
            InstanceInfo instanceInfo) {
        this.configuration = config;
        this.backupMetrics = backupMetrics;
        String ec2_region = instanceInfo.getRegion();
        snsClient =
                AmazonSNSClient.builder()
                        .withCredentials(iamCredential.getAwsCredentialProvider())
                        .withRegion(ec2_region)
                        .build();
    }

    @Override
    public void notify(
            final String msg, final Map<String, MessageAttributeValue> messageAttributes) {
        // e.g. arn:aws:sns:eu-west-1:1234:eu-west-1-cass-sample-backup
        final String topic_arn = this.configuration.getBackupNotificationTopicArn();
        if (!configuration.enableBackupNotification() || StringUtils.isEmpty(topic_arn)) {
            return;
        }

        PublishResult publishResult;
        try {
            publishResult =
                    new BoundedExponentialRetryCallable<PublishResult>() {
                        @Override
                        public PublishResult retriableCall() throws Exception {
                            PublishRequest publishRequest =
                                    new PublishRequest(topic_arn, msg)
                                            .withMessageAttributes(messageAttributes);
                            return snsClient.publish(publishRequest);
                        }
                    }.call();

        } catch (Exception e) {
            logger.error(
                    String.format(
                            "Exhausted retries.  Publishing notification metric for failure and moving on.  Failed msg to publish: %s",
                            msg),
                    e);
            backupMetrics.incrementSnsNotificationFailure();
            return;
        }

        // If here, message was published.  As a extra validation, ensure we have a msg id
        String publishedMsgId = publishResult.getMessageId();
        if (publishedMsgId == null || publishedMsgId.isEmpty()) {
            backupMetrics.incrementSnsNotificationFailure();
            return;
        }

        backupMetrics.incrementSnsNotificationSuccess();
        if (logger.isTraceEnabled()) {
            logger.trace("Published msg:  {} aws sns messageId - {}", msg, publishedMsgId);
        }
    }
}
