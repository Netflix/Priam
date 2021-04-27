package com.netflix.priam.health;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestThriftChecker {
    private IConfiguration config;

    @Mocked private Process mockProcess;

    @Before
    public void TestThriftChecker() {
        Injector injector = Guice.createInjector(new BRTestModule());
        config = injector.getInstance(IConfiguration.class);
    }

    @Test
    public void testThriftServerIsListeningDisabled() {
        ((FakeConfiguration) config).setCheckThriftServerIsListening(false);
        ThriftChecker checker = new ThriftChecker(config);
        Assert.assertTrue(checker.isThriftServerListening());
    }

    @Test
    public void testThriftServerIsNotListening() {
        ((FakeConfiguration) config).setCheckThriftServerIsListening(true);
        ThriftChecker checker = new ThriftChecker(config);
        Assert.assertFalse(checker.isThriftServerListening());
    }

    @Test
    public void testThriftServerIsListening() throws IOException {
        ((FakeConfiguration) config).setCheckThriftServerIsListening(true);
        ThriftChecker checker = new ThriftChecker(config);
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

        Assert.assertTrue(checker.isThriftServerListening());
    }

    @Test
    public void testThriftServerIsListeningException() throws IOException {
        ((FakeConfiguration) config).setCheckThriftServerIsListening(true);
        ThriftChecker checker = new ThriftChecker(config);
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

        Assert.assertTrue(checker.isThriftServerListening());
    }
}
