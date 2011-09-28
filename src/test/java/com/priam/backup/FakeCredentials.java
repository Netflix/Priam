package com.priam.backup;

import com.netflix.aws.AWSManager;
import com.netflix.library.NFLibraryManager;
import com.netflix.platform.core.PlatformManager;
import com.priam.aws.ICredential;

public class FakeCredentials implements ICredential
{
    AWSManager awsm;

    public FakeCredentials()
    {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("platform.ListOfComponentsToInit", "AWS,LOGGING");
        props.setProperty("netflix.environment", "test");
        props.setProperty("netflix.appinfo.name", "testapp");

        try
        {
            NFLibraryManager.initLibrary(PlatformManager.class, props, true, false);
        }
        catch (Exception e)
        {
            // eat it
        }
        awsm = AWSManager.getInstance();
    }

    @Override
    public String getAccessKeyId()
    {
        // TODO Auto-generated method stub
        return awsm.getAccessKeyId();
    }

    @Override
    public String getSecretAccessKey()
    {
        // TODO Auto-generated method stub
        return awsm.getSecretAccessKey();
    }

}
