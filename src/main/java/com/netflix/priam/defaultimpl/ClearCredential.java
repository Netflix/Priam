package com.netflix.priam.defaultimpl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.netflix.priam.ICredential;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * This is a basic implementation of ICredentials. User should prefer to
 * implement their own versions for more secured access. This class requires
 * clear AWS key and access.
 * <p/>
 * Set the folowing properties in "conf/awscredntial.properties" Eg: AWSACCESSID
 * = "..." AWSKEY = "..."
 */
public class ClearCredential implements ICredential {
    private static final Logger logger = LoggerFactory.getLogger(ClearCredential.class);
    public static String CRED_FILE = "/etc/awscredential.properties";

    private final AWSCredentials credentials;

    public ClearCredential() {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(CRED_FILE);
            Properties props = new Properties();
            props.load(fis);
            String accessId = props.getProperty("AWSACCESSID");
            String awsKey = props.getProperty("AWSKEY");
            credentials = new BasicAWSCredentials(accessId, awsKey);
        } catch (Exception e) {
            logger.error("Exception with credential file ", e);
            throw new RuntimeException("Problem reading credential file. Cannot start.", e);
        } finally {
            FileUtils.closeQuietly(fis);
        }
    }

    @Override
    public AWSCredentials getCredentials() {
        return credentials;
    }
}
