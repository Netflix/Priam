package com.netflix.priam.defaultimpl;

import java.io.FileInputStream;
import java.util.Properties;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.ICredential;

/**
 * This is a basic implementation of ICredentials. User should prefer to
 * implement their own versions for more secured access. This class requires
 * clear AWS key and access.
 * 
 * Set the following properties in "conf/awscredntial.properties" 
 * 
 */
public class ClearCredential implements ICredential
{
    private static final Logger logger = LoggerFactory.getLogger(ClearCredential.class);
    public static String CRED_FILE = "/etc/awscredential.properties";
    private Properties props;

    public ClearCredential()
    {
        FileInputStream fis = null;
        try
        {
            fis = new FileInputStream(CRED_FILE);
            props = new Properties();
            props.load(fis);
        }
        catch (Exception e)
        {
            logger.error("Exception with credential file ", e);
            throw new RuntimeException("Problem reading credential file. Cannot start.", e);
        }
        finally
        {
            FileUtils.closeQuietly(fis);
        }

    }

    @Override
    public String getAccessKeyId()
    {
        return props.getProperty("AWSACCESSID");
    }

    @Override
    public String getSecretAccessKey()
    {
        return props.getProperty("AWSKEY");
    }

    public AWSCredentials getCredentials()
    {
        return new BasicAWSCredentials(getAccessKeyId(), getSecretAccessKey());
    }

	@Override
	public AWSCredentialsProvider getAwsCredentialProvider() {
		return new AWSCredentialsProvider(){
			public AWSCredentials getCredentials(){
				return getCredentials();				
			}

			@Override
			public void refresh() {
				// NOP				
			}
		};
	}
}
