package com.priam.netflix;

import java.io.IOException;
import java.io.PrintWriter;

import com.netflix.server.base.BaseStatusPage;
import com.priam.conf.JMXNodeTool;
import com.priam.conf.PriamServer;

public class StatusPage extends BaseStatusPage
{

    //** TO DO ** Use BasePageRenderer and register it with StatusRegistry
    private static final long serialVersionUID = 1L;

    public void getDetails(PrintWriter out, boolean htmlize)
    {
        JMXNodeTool tool = JMXNodeTool.instance(PriamServer.instance.config);
        try
        {
            out.write(tool.info().toString());
        }
        finally
        {
            try
            {
                tool.close();
            }
            catch (IOException e)
            {
                // Do nothing.
            }
        }
    }

}
