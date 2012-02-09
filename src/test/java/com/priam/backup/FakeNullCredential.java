package com.priam.backup;


import com.amazonaws.auth.AWSCredentials;

public class FakeNullCredential implements AWSCredentials {

    @Override
    public String getAWSAccessKeyId() {
        return "testkey";
    }

    @Override
    public String getAWSSecretKey() {
        return "testid";
    }
}
