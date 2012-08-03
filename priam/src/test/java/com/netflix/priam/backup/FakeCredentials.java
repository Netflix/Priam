package com.netflix.priam.backup;

import com.netflix.priam.ICredential;

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
