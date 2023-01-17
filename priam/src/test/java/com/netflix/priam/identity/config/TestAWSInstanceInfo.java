package com.netflix.priam.identity.config;

import com.google.common.truth.Truth;
import mockit.Expectations;
import org.junit.Before;
import org.junit.Test;

/** tests of {@link com.netflix.priam.identity.config.AWSInstanceInfo} */
public class TestAWSInstanceInfo {
    private AWSInstanceInfo instanceInfo;

    @Before
    public void setUp() {
        instanceInfo =
                new AWSInstanceInfo(
                        () -> {
                            throw new RuntimeException("not implemented");
                        });
    }

    @Test
    public void testPublicHostIP() {
        new Expectations(instanceInfo) {
            {
                instanceInfo.tryGetDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTIP_URL);
                result = "1.2.3.4";
            }
        };
        Truth.assertThat(instanceInfo.getHostIP()).isEqualTo("1.2.3.4");
    }

    @Test
    public void testMissingPublicHostIP() {
        new Expectations(instanceInfo) {
            {
                instanceInfo.tryGetDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTIP_URL);
                result = null;
                instanceInfo.tryGetDataFromUrl(AWSInstanceInfo.LOCAL_HOSTIP_URL);
                result = "1.2.3.4";
            }
        };
        Truth.assertThat(instanceInfo.getHostIP()).isEqualTo("1.2.3.4");
    }

    @Test
    public void testPublicHostname() {
        new Expectations(instanceInfo) {
            {
                instanceInfo.tryGetDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTNAME_URL);
                result = "hostname";
            }
        };
        Truth.assertThat(instanceInfo.getHostname()).isEqualTo("hostname");
    }

    @Test
    public void testMissingPublicHostname() {
        new Expectations(instanceInfo) {
            {
                instanceInfo.tryGetDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTNAME_URL);
                result = null;
                instanceInfo.tryGetDataFromUrl(AWSInstanceInfo.LOCAL_HOSTNAME_URL);
                result = "hostname";
            }
        };
        Truth.assertThat(instanceInfo.getHostname()).isEqualTo("hostname");
    }
}
