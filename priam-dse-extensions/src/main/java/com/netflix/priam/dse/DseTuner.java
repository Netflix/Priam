package com.netflix.priam.dse;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.defaultimpl.StandardTuner;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

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

    protected String getSnitch()
    {
        return dseConfig.getDseDelegatingSnitch();
    }
}
