package com.netflix.priam.backup;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.netflix.priam.ICredential;

public class FakeCredentials implements ICredential
{
    @Override
    public AWSCredentials getCredentials() {
        return new BasicAWSCredentials("", "");
    }
}
