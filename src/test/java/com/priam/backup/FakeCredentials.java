package com.priam.backup;

import com.priam.aws.ICredential;

public class FakeCredentials implements ICredential
{

    @Override
    public String getAccessKeyId()
    {
        return "";
    }

    @Override
    public String getSecretAccessKey()
    {
        return "";
    }

}
