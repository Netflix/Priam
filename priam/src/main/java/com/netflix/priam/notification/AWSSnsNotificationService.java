package com.netflix.priam.notification;

import com.amazonaws.services.sns.AmazonSNS;
import com.netflix.priam.IConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.aws.IAMCredential;
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;

/*
 * A single, persisted, connection to Amazon SNS.
 */
@Singleton
public class AWSSnsNotificationService implements INotificationService {
	private static final Logger logger = LoggerFactory.getLogger(AWSSnsNotificationService.class);

	private IConfiguration configuration;
	private AmazonSNS snsClient;
	private IMetricPublisher metricPublisher;
	private IMeasurement<Object> notificationMeasurement;
	
	@Inject
	public AWSSnsNotificationService(IConfiguration config, IAMCredential iamCredential
			, IMetricPublisher metricPublisher) {
		this.configuration = config;
		this.metricPublisher = metricPublisher;
		this.notificationMeasurement = new NoticationMeasurement();
		String ec2_region = this.configuration.getDC();
		snsClient = AmazonSNSClient.builder()
				.withCredentials(iamCredential.getAwsCredentialProvider())
				.withRegion(ec2_region).build();
	}
	
	@Override
	public void notify(final String msg) {
		final String topic_arn = this.configuration.getBackupNotificationTopicArn(); //e.g. arn:aws:sns:eu-west-1:1234:eu-west-1-cass-sample-backup
		if (StringUtils.isEmpty(topic_arn)) {
			return;
		}
		
		PublishResult publishResult = null;
		try {
			publishResult = new BoundedExponentialRetryCallable<PublishResult>() {
				@Override
				public PublishResult retriableCall() throws Exception {
					PublishRequest publishRequest = new PublishRequest(topic_arn, msg);
					PublishResult result = snsClient.publish(publishRequest);
					return result;
				}
			}.call();
			
		} catch (Exception e) {
			logger.error(String.format("Exhausted retries.  Publishing notification metric for failure and moving on.  Failed msg to publish: {}", msg), e);
			this.notificationMeasurement.incrementFailureCnt(1);
			this.metricPublisher.publish(this.notificationMeasurement);
			return;
		}

		//If here, message was published.  As a extra validation, ensure we have a msg id
		String publishedMsgId = publishResult.getMessageId();
		if (publishedMsgId == null || publishedMsgId.isEmpty() ) {
			this.notificationMeasurement.incrementFailureCnt(1);
			this.metricPublisher.publish(this.notificationMeasurement);
			return;
		}

		this.notificationMeasurement.incrementSuccessCnt(1);
		this.metricPublisher.publish(this.notificationMeasurement);
		if (logger.isDebugEnabled()) {
			logger.debug("Published msg:  {} aws sns messageId - {}", msg, publishedMsgId);
		}
	}
	
	public class NoticationMeasurement implements IMeasurement<Object> {
		private int falureCnt = 0, successCnt = 0;
		@Override
		public int getFailureCnt() {
			return this.falureCnt;
		}
		@Override
		public int getSuccessCnt() {
			return this.successCnt;
		}

		@Override
		public com.netflix.priam.merics.IMeasurement.MMEASUREMENT_TYPE getType() {
			return IMeasurement.MMEASUREMENT_TYPE.SNAPSHOTBACKUPUPNOTIFICATION;
		}

		@Override
		public Object getVal() {
			return null;
		}

		@Override
		public void incrementFailureCnt(int val) {
			this.falureCnt += val;
		}

		@Override
		public void incrementSuccessCnt(int val) {
			this.successCnt += val;
		}

		@Override
		public void setVal(Object arg0) {
			//TODO  Auto-generated method stub
			
		}
	}
	
}