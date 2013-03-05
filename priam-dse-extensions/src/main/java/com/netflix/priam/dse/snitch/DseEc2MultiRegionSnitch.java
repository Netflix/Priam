package com.netflix.priam.dse.snitch;

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.locator.Ec2MultiRegionSnitch;

/**
 * NOTE: this class is a temporary stand-in until CASSANDRA-5155 is sorted out
 * and imported into Datastax Enterprise.
 */
public class DseEc2MultiRegionSnitch extends Ec2MultiRegionSnitch
{
    public DseEc2MultiRegionSnitch() throws IOException, ConfigurationException
    {
        super();
        String datacenterSuffix = SnitchProperties.get("dc_suffix");
        if(datacenterSuffix != null)
            ec2region = ec2region.concat(datacenterSuffix);
        logger.info("DseEc2MultiRegionSnitch using region: " + ec2region + ", zone: " + ec2zone + ".");
    }
}
