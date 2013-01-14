package com.netflix.priam.dse.snitch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.Ec2Snitch;
import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import static org.apache.cassandra.locator.GossipingPropertyFileSnitch.RACKDC_PROPERTY_FILENAME;

/**
 * NOTE: this class is a temporary stand-in until CASSANDRA-5155 is sorted out
 * and imported into Datastax Enterprise.
 */
public class DseEc2Snitch extends Ec2Snitch
{
    public DseEc2Snitch() throws IOException, ConfigurationException
    {
        super();
        loadConfiguration();
    }

    private void loadConfiguration() throws ConfigurationException
    {
        InputStream stream = GossipingPropertyFileSnitch.class.getClassLoader().getResourceAsStream(RACKDC_PROPERTY_FILENAME);
        Properties properties = new Properties();
        try
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Unable to read " + RACKDC_PROPERTY_FILENAME, e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet())
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.equals("dc_suffix"))
                ec2region = ec2region.concat(value);
        }
    }
}
