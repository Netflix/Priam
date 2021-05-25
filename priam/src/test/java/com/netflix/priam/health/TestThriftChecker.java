package com.netflix.priam.health;

import com.netflix.priam.config.FakeConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestThriftChecker {
    private FakeConfiguration config;
    private ThriftChecker thriftChecker;

    @Mocked private Process mockProcess;

    @Before
    public void TestThriftChecker() {
        config = new FakeConfiguration();
        thriftChecker = new ThriftChecker(config);
    }

    @Test
    public void testThriftServerIsListeningDisabled() {
        config.setCheckThriftServerIsListening(false);
        Assert.assertTrue(thriftChecker.isThriftServerListening());
    }

    @Test
    public void testThriftServerIsNotListening() {
        config.setCheckThriftServerIsListening(true);
        Assert.assertFalse(thriftChecker.isThriftServerListening());
    }

    @Test
    public void testThriftServerIsListening() throws IOException {
        config.setCheckThriftServerIsListening(true);
        final InputStream mockOutput = new ByteArrayInputStream("1".getBytes());
        new Expectations() {
            {
                mockProcess.getInputStream();
                result = mockOutput;
            }
        };
        // Mock out the ps call
        final Runtime r = Runtime.getRuntime();
        String[] cmd = {
            "/bin/sh", "-c", "ss -tuln | grep -c " + config.getThriftPort(), " 2>/dev/null"
        };
        new Expectations(r) {
            {
                r.exec(cmd);
                result = mockProcess;
            }
        };

        Assert.assertTrue(thriftChecker.isThriftServerListening());
    }

    @Test
    public void testThriftServerIsListeningException() throws IOException {
        config.setCheckThriftServerIsListening(true);
        final IOException mockOutput = new IOException("Command exited with code 0");
        new Expectations() {
            {
                mockProcess.getInputStream();
                result = mockOutput;
            }
        };
        // Mock out the ps call
        final Runtime r = Runtime.getRuntime();
        String[] cmd = {
            "/bin/sh", "-c", "ss -tuln | grep -c " + config.getThriftPort(), " 2>/dev/null"
        };
        new Expectations(r) {
            {
                r.exec(cmd);
                result = mockProcess;
            }
        };

        Assert.assertTrue(thriftChecker.isThriftServerListening());
    }
}
