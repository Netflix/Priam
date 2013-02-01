/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.cassandra.extensions;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.SeedProvider;

/**
 * Retrieves the list of seeds from Priam.
 */
public class NFSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(NFSeedProvider.class);
    List<InetAddress> seeds = new ArrayList<InetAddress>();

    public NFSeedProvider(Map<String, String> args)
    {
        try
        {
            String seedString;
            while(true)
            {

                seedString = DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_seeds");
                if(seedString != null && !seedString.isEmpty())
                    break;
                logger.info("didn't get seeds from Priam; sleeping...");
                Thread.sleep(1000);
            }
            logger.info("seed list = " + seedString);
            for (String seed : seedString.split(","))
                seeds.add(InetAddress.getByName(seed));
        }
        catch (Exception e)
        {
            logger.error("Failed to get seeds.");
        }

    }

    @Override
    public List<InetAddress> getSeeds()
    {
        return seeds;
    }
}
