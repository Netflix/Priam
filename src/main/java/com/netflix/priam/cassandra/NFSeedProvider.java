package com.netflix.priam.cassandra;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.locator.SeedProvider;

import com.netflix.priam.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NFSeedProvider implements SeedProvider
{
    List<InetAddress> return_ = new ArrayList<InetAddress>();
    private static final Logger logger = LoggerFactory.getLogger(NFSeedProvider.class);

    /**
     * Populates args with list of seeds queried from Priam
     */
    public NFSeedProvider(Map<String, String> args)
    {
        try
        {
            String seeds = SystemUtils.getDataFromUrl("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_seeds");
            logger.info("Got seed list: {}", seeds);
            for (String seed : seeds.split(","))
                return_.add(InetAddress.getByName(seed));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    @Override
    public List<InetAddress> getSeeds()
    {
        return return_;
    }
}
