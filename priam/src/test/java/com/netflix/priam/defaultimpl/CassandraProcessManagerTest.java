package com.netflix.priam.defaultimpl;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.IConfiguration;
import junit.framework.Assert;

public class CassandraProcessManagerTest
{
    CassandraProcessManager cpm;

    @Before
    public void setup()
    {
        IConfiguration config = new FakeConfiguration("us-east-1", "test_cluster", "us-east-1a", "i-2378afd3");
        cpm = new CassandraProcessManager(config);
    }

    @Test
    public void logProcessOutput_BadApp() throws IOException, InterruptedException
    {
        Process p = null;
        try
        {
            p = new ProcessBuilder("ls", "/tmppppp").start();
            int exitValue = p.waitFor();
            Assert.assertTrue(0 != exitValue);
            cpm.logProcessOutput(p);
        }
        catch(IOException ioe)
        {
            if(p!=null)
                cpm.logProcessOutput(p);
        }
    }

    /**
     * note: this will succeed on a *nix machine, unclear about anything else...
     */
    @Test
    public void logProcessOutput_GoodApp() throws IOException, InterruptedException
    {
        Process p = new ProcessBuilder("true").start();
        int exitValue = p.waitFor();
        Assert.assertEquals(0, exitValue);
        cpm.logProcessOutput(p);
    }
}
