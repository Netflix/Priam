package com.priam.defaultimpl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;

import java.io.File;
import java.io.IOException;

/**
 * This is a basic implementation of AWSCredentials. User should prefer to
 * implement their own versions for more secured access. This class requires
 * clear AWS key and access.
 * <p/>
 * Set the folowing properties in "conf/awscredntial.properties"
 * Eg:
 * AWSACCESSID = "..."
 * AWSKEY = "..."
 *
 * @author Praveen Sadhu
 */
public class ClearCredential implements AWSCredentials {

    public static final String CRED_FILE = "conf/awscredential.properties";
    private final AWSCredentials credentials;

    public ClearCredential() throws IOException {
        credentials = new PropertiesCredentials(new File(CRED_FILE));
    }

    @Override
    public String getAWSAccessKeyId() {
        return credentials.getAWSAccessKeyId();
    }

    @Override
    public String getAWSSecretKey() {
        return credentials.getAWSSecretKey();
    }
}
