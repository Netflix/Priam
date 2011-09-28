package com.priam.backup;

import com.priam.aws.ICredential;

public class FakeNullCredential implements ICredential
{

    @Override
    public String getAccessKeyId()
    {
        return "testid";
    }

    @Override
    public String getSecretAccessKey()
    {
        return "testkey";
    }

}
