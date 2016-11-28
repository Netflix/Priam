package com.netflix.priam.aws;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.priam.merics.AWSSlowDownExceptionMeasurement;
import com.netflix.priam.merics.BackupUploadRateMeasurement;
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;

public class S3FileSystemBase {
	private static final Logger logger = LoggerFactory.getLogger(S3FileSystemBase.class); 
	
	protected static final int MAX_CHUNKS = 10000;
    protected static final long MAX_BUFFERED_IN_STREAM_SIZE = 5 * 1024 * 1024;
	
	protected AtomicInteger uploadCount = new AtomicInteger();
	protected AtomicLong bytesUploaded = new AtomicLong(); //bytes uploaded per file
    protected AtomicInteger downloadCount = new AtomicInteger();
    protected AtomicLong bytesDownloaded = new AtomicLong();
	protected AmazonS3Client s3Client;
    protected IMetricPublisher metricPublisher;
    protected IMeasurement awsSlowDownMeasurement;
    protected int awsSlowDownExceptionCounter = 0;

    public S3FileSystemBase (IMetricPublisher metricPublisher) {
        this.metricPublisher = metricPublisher;
        awsSlowDownMeasurement = new AWSSlowDownExceptionMeasurement(); //a counter of AWS warning for all uploads
    }

    /*
     * S3 End point information
     * http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
     */
    protected String getS3Endpoint(IConfiguration config)
    {
    	 final String curRegion = config.getDC();
         if("us-east-1".equalsIgnoreCase(curRegion) ||
            "us-west-1".equalsIgnoreCase(curRegion) ||
            "us-west-2".equalsIgnoreCase(curRegion)	|| 
            "eu-west-1".equalsIgnoreCase(curRegion) ||
            "sa-east-1".equalsIgnoreCase(curRegion) ||
            "eu-central-1".equalsIgnoreCase(curRegion))
             return config.getS3EndPoint();
         
         throw new IllegalStateException("Unsupported region for this application: " + curRegion);
    }
    
    public AmazonS3 getS3Client()
    {
        return s3Client;
    }    
    
    /**
     * Get S3 prefix which will be used to locate S3 files
     */
    protected String getPrefix(IConfiguration config)
    {
        String prefix;
        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();

        String[] paths = prefix.split(String.valueOf(S3BackupPath.PATH_SEP));
        return paths[0];
    }  
    
    protected void cleanUp(IConfiguration config, Provider<AbstractBackupPath> pathProvider) {
    	
        AmazonS3 s3Client = getS3Client();
        String clusterPath = pathProvider.get().clusterPrefix("");
        logger.debug("Bucket: " + config.getBackupPrefix());
        BucketLifecycleConfiguration lifeConfig = s3Client.getBucketLifecycleConfiguration(config.getBackupPrefix());
        logger.debug("Got bucket:" + config.getBackupPrefix() + " lifecycle." + lifeConfig);
        if (lifeConfig == null)
        {
            lifeConfig = new BucketLifecycleConfiguration();
            List<Rule> rules = Lists.newArrayList();
            lifeConfig.setRules(rules);
        }
        List<Rule> rules = lifeConfig.getRules();
        if (updateLifecycleRule(config, rules, clusterPath))
        {
            if( rules.size() > 0 ){
                lifeConfig.setRules(rules);
                s3Client.setBucketLifecycleConfiguration(config.getBackupPrefix(), lifeConfig);
            }
            else
                s3Client.deleteBucketLifecycleConfiguration(config.getBackupPrefix());
        }    	
    	
    }
    
    private boolean updateLifecycleRule(IConfiguration config, List<Rule> rules, String prefix)
    {
        Rule rule = null;
        for (BucketLifecycleConfiguration.Rule lcRule : rules)
        {
            if (lcRule.getPrefix().equals(prefix))
            {
                rule = lcRule;
                break;
            }
        }
        if (rule == null && config.getBackupRetentionDays() <= 0)
            return false;
        if (rule != null && rule.getExpirationInDays() == config.getBackupRetentionDays())
        {
            logger.info("Cleanup rule already set");
            return false;
        }
        if (rule == null)
        {
            // Create a new rule
            rule = new BucketLifecycleConfiguration.Rule().withExpirationInDays(config.getBackupRetentionDays()).withPrefix(prefix);
            rule.setStatus(BucketLifecycleConfiguration.ENABLED);
            rule.setId(prefix);
            rules.add(rule);
            logger.info(String.format("Setting cleanup for %s to %d days", rule.getPrefix(), rule.getExpirationInDays()));
        }
        else if (config.getBackupRetentionDays() > 0)
        {
            logger.info(String.format("Setting cleanup for %s to %d days", rule.getPrefix(), config.getBackupRetentionDays()));
            rule.setExpirationInDays(config.getBackupRetentionDays());
        }
        else
        {
            logger.info(String.format("Removing cleanup rule for %s", rule.getPrefix()));
            rules.remove(rule);
        }
        return true;
    }

    /*
    @param path - representation of the file uploaded
    @param start time of upload, in millisecs
    @param completion time of upload, in millsecs
     */
    protected void postProcessingPerFile(AbstractBackupPath path, long startTimeInMilliSecs, long completedTimeInMilliSecs) {
        //Publish upload rate for each uploaded file
        try {
            long sizeInBytes = path.getSize();
            long elapseTimeInMillisecs = completedTimeInMilliSecs - startTimeInMilliSecs;
            long elapseTimeInSecs = elapseTimeInMillisecs / 1000; //converting millis to seconds as 1000m in 1 second
            long bytesReadPerSec = 0;
            Double speedInKBps = 0.0;
            if (elapseTimeInSecs> 0 && sizeInBytes > 0) {
                bytesReadPerSec = sizeInBytes / elapseTimeInSecs;
                speedInKBps = bytesReadPerSec / 1024D;
            } else {
                bytesReadPerSec = sizeInBytes;  //we uploaded the whole file in less than a sec
                speedInKBps = (double) sizeInBytes;
            }

            logger.info("Upload rate for file: " + path.getFileName()
                    + ", elapsse time in sec(s): " + elapseTimeInSecs
                    + ", KB per sec: " + speedInKBps
            );

            /*
            This measurement is different than most others.  Other measurements are applicable to all occurrences (e.g
            node tool flush errors, AWS TPS warning errors).  Upload rate for all occurrences (uploads) is not useful; rather,
            we are interested in the upload rate per file.  Hence "metadata" is the upload rate for the just uploaded file.
             */
            IMeasurement backupUploadRateMeasurement = new BackupUploadRateMeasurement();
            BackupUploadRateMeasurement.Metadata metadata = new BackupUploadRateMeasurement.Metadata(path.getFileName(), speedInKBps, elapseTimeInMillisecs);
            backupUploadRateMeasurement.setVal(metadata);
            this.metricPublisher.publish(backupUploadRateMeasurement); //signal of upload rate for file

            awsSlowDownMeasurement.incrementFailureCnt(path.getAWSSlowDownExceptionCounter());
            this.metricPublisher.publish(awsSlowDownMeasurement); //signal of possible throttling by aws

        } catch (Exception e) {
            logger.error("Post processing of file " + path.getFileName() + " failed, not fatal.", e);
        }
    }

    /*
    Reinitializtion which should be performed before uploading a file
     */
    protected void reinitialize() {
        bytesUploaded = new AtomicLong(0); //initi
        this.awsSlowDownExceptionCounter = 0;
    }
}