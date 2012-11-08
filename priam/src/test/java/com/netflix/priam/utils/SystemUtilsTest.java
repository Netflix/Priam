package com.netflix.priam.utils;

import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;

public class SystemUtilsTest
{
    @Test
    public void logProcessOutput_BadApp() throws IOException, InterruptedException
    {
        Process p = null;
        try
        {
            p = new ProcessBuilder("ls", "/tmppppp").start();
            int exitValue = p.waitFor();
            Assert.assertTrue(0 != exitValue);
            SystemUtils.logProcessOutput(p);
        }
        catch(IOException ioe)
        {
            if(p!=null)
                SystemUtils.logProcessOutput(p);
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
        SystemUtils.logProcessOutput(p);
    }
}
