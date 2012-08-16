package com.netflix.priam.cassandra;

import com.netflix.priam.utils.SystemUtils;
import org.apache.cassandra.locator.SeedProvider;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NFSeedProvider implements SeedProvider {
    List<InetAddress> return_ = new ArrayList<InetAddress>();

    /**
     * Populates args with list of seeds queried from Priam
     */
    public NFSeedProvider(Map<String, String> args) {
        try {
            String seeds = SystemUtils.getDataFromUrl("http://127.0.0.1:8080/v1/cassconfig/get_seeds");
            for (String seed : seeds.split(",")) {
                return_.add(InetAddress.getByName(seed));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public List<InetAddress> getSeeds() {
        return return_;
    }
}
