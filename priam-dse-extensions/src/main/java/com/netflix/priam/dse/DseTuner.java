package com.netflix.priam.dse;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.defaultimpl.StandardTuner;
import org.apache.cassandra.io.util.FileUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import static com.netflix.priam.dse.IDseConfiguration.NodeType;
import static org.apache.cassandra.locator.GossipingPropertyFileSnitch.RACKDC_PROPERTY_FILENAME;

/**
 * Makes Datastax Enterprise-specific changes to the c* yaml and dse-yaml.
 *
 * @author jason brown
 */
public class DseTuner extends StandardTuner
{
    private static final Logger logger = LoggerFactory.getLogger(DseTuner.class);
    private final IDseConfiguration dseConfig;

    @Inject
    public DseTuner(IConfiguration config, IDseConfiguration dseConfig)
    {
        super(config);
        this.dseConfig = dseConfig;
    }

    public void updateYaml(String yamlLocation, String hostname, String seedProvider) throws IOException
    {
        super.updateYaml(yamlLocation, hostname, seedProvider);
        writeDseYaml();
        writeCassandraSnitchProperties();
    }

    private void writeDseYaml() throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        String dseYaml = dseConfig.getDseYamlLocation();
        @SuppressWarnings("rawtypes")
        Map map = (Map) yaml.load(new FileInputStream(dseYaml));
        map.put("delegated_snitch", config.getSnitch());
        logger.info("Updating dse-yaml: " + yaml.dump(map));
        yaml.dump(map, new FileWriter(dseYaml));
    }

    private void writeCassandraSnitchProperties()
    {
        final NodeType nodeType = dseConfig.getNodeType();
        if(nodeType == NodeType.REAL_TIME_QUERY)
            return;

        Reader reader = null;
        try
        {
            String filePath = config.getCassHome() + "/conf/" + RACKDC_PROPERTY_FILENAME;
            reader = new FileReader(filePath);
            Properties properties = new Properties();
            properties.load(reader);
            String suffix = "";
            if(nodeType == NodeType.SEARCH)
                suffix = "_solr";
            if(nodeType == NodeType.ANALYTIC)
                suffix = "_hadoop";
            properties.put("dc_suffix", suffix);
            properties.store(new FileWriter(filePath), "");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to read " + RACKDC_PROPERTY_FILENAME, e);
        }
        finally
        {
            FileUtils.closeQuietly(reader);
        }

    }

    protected String getSnitch()
    {
        return dseConfig.getDseDelegatingSnitch();
    }
}
