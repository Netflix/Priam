package com.priam.netflix;

import java.io.IOException;

import org.apache.cassandra.dht.BigIntegerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.server.base.BaseHealthCheckServlet;
import com.priam.conf.JMXNodeTool;
import com.priam.conf.PriamServer;

public class HealthCheckServlet extends BaseHealthCheckServlet
{
    private static final long serialVersionUID = -2170603830579514231L;
    private static final Logger logger = LoggerFactory.getLogger(HealthCheckServlet.class);

    @Override
    protected boolean checkHealth()
    {
        JMXNodeTool tool = JMXNodeTool.instance(PriamServer.instance.config);
        try
        {
            BigIntegerToken token = new BigIntegerToken(tool.getToken());
            String ip = tool.getTokenToEndpointMap().get(token);
            return tool.getLiveNodes().contains(ip) && !tool.getLeavingNodes().contains(ip);
        }
        catch (Exception e)
        {
            logger.error("Exception while fetching the status", e);
        }
        finally
        {
            try
            {
                tool.close();
            }
            catch (IOException e)
            {
                // do nothing.
            }
        }
        return false;
    }
}
