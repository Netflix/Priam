package com.netflix.priam.defaultimpl;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.cassandra.io.util.FileUtils;

import com.netflix.priam.ICredential;

/**
 * This is a basic implementation of ICredentials. User should prefer to
 * implement their own versions for more secured access. This class requires
 * clear AWS key and access.
 * 
 * Set the folowing properties in "conf/awscredntial.properties"          
 * Eg:
 * AWSACCESSID = "..." 
 * AWSKEY = "..."
 * 
 */
public class ClearCredential implements ICredential
{

    public static String CRED_FILE = "conf/awscredential.properties";
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

}
