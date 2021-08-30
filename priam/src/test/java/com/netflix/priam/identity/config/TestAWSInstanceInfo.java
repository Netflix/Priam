package com.netflix.priam.identity.config;

import com.google.common.truth.Truth;
import com.netflix.priam.utils.SystemUtils;
import mockit.Expectations;
import mockit.Mocked;
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
    public void testPublicHostIP(@Mocked SystemUtils systemUtils) {
        new Expectations() {
            {
                SystemUtils.getDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTIP_URL);
                result = "1.2.3.4";
            }
        };
        Truth.assertThat(instanceInfo.getHostIP()).isEqualTo("1.2.3.4");
    }

    @Test
    public void testMissingPublicHostIP(@Mocked SystemUtils systemUtils) {
        new Expectations() {
            {
                SystemUtils.getDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTIP_URL);
                result = new RuntimeException();
                SystemUtils.getDataFromUrl(AWSInstanceInfo.LOCAL_HOSTIP_URL);
                result = "1.2.3.4";
            }
        };
        Truth.assertThat(instanceInfo.getHostIP()).isEqualTo("1.2.3.4");
    }

    @Test
    public void testPublicHostname(@Mocked SystemUtils systemUtils) {
        new Expectations() {
            {
                SystemUtils.getDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTNAME_URL);
                result = "hostname";
            }
        };
        Truth.assertThat(instanceInfo.getHostname()).isEqualTo("hostname");
    }

    @Test
    public void testMissingPublicHostname(@Mocked SystemUtils systemUtils) {
        new Expectations() {
            {
                SystemUtils.getDataFromUrl(AWSInstanceInfo.PUBLIC_HOSTNAME_URL);
                result = new RuntimeException();
                SystemUtils.getDataFromUrl(AWSInstanceInfo.LOCAL_HOSTNAME_URL);
                result = "hostname";
            }
        };
        Truth.assertThat(instanceInfo.getHostname()).isEqualTo("hostname");
    }
}
