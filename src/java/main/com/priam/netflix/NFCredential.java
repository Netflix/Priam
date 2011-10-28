package com.priam.netflix;

import com.netflix.aws.AWSManager;
import com.priam.aws.ICredential;

public class NFCredential implements ICredential
{

    @Override
    public String getAccessKeyId()
    {
        return AWSManager.getInstance().getAccessKeyId();
    }

    @Override
    public String getSecretAccessKey()
    {
        return AWSManager.getInstance().getSecretAccessKey();
    }

}
