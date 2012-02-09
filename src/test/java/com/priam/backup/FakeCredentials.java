package com.priam.backup;

import com.amazonaws.auth.AWSCredentials;

public class FakeCredentials implements AWSCredentials {
    @Override
    public String getAWSAccessKeyId() {
        return "";
    }

    @Override
    public String getAWSSecretKey() {
        return "";
    }
}
