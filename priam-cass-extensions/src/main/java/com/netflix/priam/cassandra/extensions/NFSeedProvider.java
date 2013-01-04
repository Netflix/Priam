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

import org.apache.cassandra.locator.SeedProvider;

/**
 * Retrieves the list of seeds from Priam.
 */
public class NFSeedProvider implements SeedProvider
{
    List<InetAddress> return_ = new ArrayList<InetAddress>();

    public NFSeedProvider(Map<String, String> args)
    {
        try
        {
            String seeds = DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_seeds");
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
