package com.netflix.priam.cassandra.thrift;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.locator.SeedProvider;

public class NFSeedProvider implements SeedProvider {
	List<InetAddress> return_ = new ArrayList<InetAddress>();

	/**
	 * Populates args with list of seeds queried from Priam
	 * @param args
	 */
    public NFSeedProvider(Map<String, String> args)
    {
		try {
			String seeds = AWSUtil.getDataFromUrl("http://127.0.0.1:8080/Priam/REST/cassandra_config/test?type=GET_SEEDS");
			for (String seed : seeds.split(","))
				return_.add(InetAddress.getByName(seed));
		} catch (Exception e) {
			e.printStackTrace();
		}

    }
    
	@Override
	public List<InetAddress> getSeeds() {
		return return_;
	}
}
