package com.netflix.priam.cassandra;

import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.utils.SystemUtils;

public class NFThinCassandraDaemon extends CassandraDaemon
{
    private static final Logger logger = LoggerFactory.getLogger(NFThinCassandraDaemon.class);

    public static void main(String[] args)
    {
        String token = null;
        String seeds = null;
        boolean isReplace = false;
        while (true)
        {
            try
            {
                token = SystemUtils.getDataFromUrl("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/GET_TOKEN");
                seeds = SystemUtils.getDataFromUrl("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/GET_SEEDS");
                isReplace = Boolean.parseBoolean(SystemUtils.getDataFromUrl("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/IS_REPLACE_TOKEN"));
            }
            catch (Exception e)
            {
                logger.error("Failed to obtain a token from a pre-defined list, we can not start!", e);
            }

            if (token != null && seeds != null)
                break;
            // sleep for 1 sec and try again.
            try
            {
                Thread.sleep(1 * 1000);
            }
            catch (InterruptedException e1)
            {
                // do nothing.
            }
        }
        System.setProperty("cassandra.initial_token", token);

        if (isReplace)
            System.setProperty("cassandra.replace_token", token);

        new CassandraDaemon().activate();
    }
}
