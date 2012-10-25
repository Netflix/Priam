package com.netflix.priam.backup;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.netflix.priam.ICredential;

public class FakeCredentials implements ICredential
{
    @Override
    public AWSCredentialsProvider getCredentials() {
        return new StaticCredentialsProvider(new BasicAWSCredentials("", ""));
    }
}
