package com.netflix.priam.dse;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.defaultimpl.StandardTuner;
import org.apache.cassandra.io.util.FileUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import static com.netflix.priam.dse.IDseConfiguration.NodeType;
import static org.apache.cassandra.locator.SnitchProperties.RACKDC_PROPERTY_FILENAME;

/**
 * Makes Datastax Enterprise-specific changes to the c* yaml and dse-yaml.
 *
 * @author jason brown
 * @author minh do
 */
public class DseTuner extends StandardTuner
{
    private static final Logger logger = LoggerFactory.getLogger(DseTuner.class);
    protected static final String AUDIT_LOG_FILE = "/conf/log4j-server.properties";

    protected static final String PRIMARY_AUDIT_LOG_ENTRY = "log4j.logger.DataAudit";
    protected static final String AUDIT_LOG_ADDITIVE_ENTRY = "log4j.additivity.DataAudit";

    protected static final String AUDIT_LOG_DSE_ENTRY = "audit_logging_options";

    private final IDseConfiguration dseConfig;

    @Inject
    public DseTuner(IConfiguration config, IDseConfiguration dseConfig)
    {
        super(config);
        this.dseConfig = dseConfig;
    }

    public void writeAllProperties(String yamlLocation, String hostname, String seedProvider) throws IOException
    {
        super.writeAllProperties(yamlLocation, hostname, seedProvider);
        writeDseYaml();
        writeCassandraSnitchProperties();
        writeAuditLogProperties();
    }

    @SuppressWarnings("unchecked")
    /*package protected*/ void writeDseYaml() throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        String dseYaml = dseConfig.getDseYamlLocation();
        Map<String, Object> map = (Map<String, Object>) yaml.load(new FileInputStream(dseYaml));

        if (map.containsKey(AUDIT_LOG_DSE_ENTRY))
        {
            Boolean isEnabled = (Boolean) ((Map<String, Object>) map.get(AUDIT_LOG_DSE_ENTRY)).get("enabled");

            // Enable/disable audit logging (need this in addition to log4j-server.properties settings)
            if (dseConfig.isAuditLogEnabled())
            {
                if (!isEnabled)
                {
                    ((Map<String, Object>) map.get(AUDIT_LOG_DSE_ENTRY)).put("enabled", true);
                }
            }
            else if (isEnabled)
            {
                ((Map<String, Object>) map.get(AUDIT_LOG_DSE_ENTRY)).put("enabled", false);
            }
        }

        logger.info("Updating dse-yaml:\n" + yaml.dump(map));
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
            if(nodeType == NodeType.ANALYTIC_HADOOP)
                suffix = "_hadoop";
            if(nodeType == NodeType.ANALYTIC_HADOOP_SPARK)
                suffix = "_hadoop_spark";
            if(nodeType == NodeType.ANALYTIC_SPARK)
                suffix = "_spark";
            
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

    /**
     * Note: supporting the direct hacking of a log4j props file is far from elegant,
     * but seems less odious than other solutions I've come up with.
     * Operates under the assumption that the only people mucking with the audit log
     * entries in the value are DataStax themselves and this program, and that the original
     * property names are somehow still preserved. Otherwise, YMMV.
     */
    protected void writeAuditLogProperties()
    {
        BufferedWriter writer = null;
        try
        {
            final File srcFile = new File(config.getCassHome() + AUDIT_LOG_FILE);
            final List<String> lines = Files.readLines(srcFile, Charset.defaultCharset());
            final File backupFile = new File(config.getCassHome() + AUDIT_LOG_FILE + "." + System.currentTimeMillis());
            Files.move(srcFile, backupFile);
            writer = Files.newWriter(srcFile, Charset.defaultCharset());

            String loggerPrefix = "log4j.appender.";
            try
            {
                loggerPrefix += findAuditLoggerName(lines);
            }
            catch (IllegalStateException ise)
            {
                logger.warn(String.format("cannot locate %s property, will ignore any audit log updating", PRIMARY_AUDIT_LOG_ENTRY));
                return;
            }

            for(String line : lines)
            {
                if(line.contains(loggerPrefix) || line.contains(PRIMARY_AUDIT_LOG_ENTRY) || line.contains(AUDIT_LOG_ADDITIVE_ENTRY))
                {
                    if(dseConfig.isAuditLogEnabled())
                    {
                        //first, check to see if we need to uncomment the line
                        while(line.startsWith("#"))
                        {
                            line = line.substring(1);
                        }

                        //next, check if we need to change the prop's value
                        if(line.contains("ActiveCategories"))
                        {
                            final String cats = Joiner.on(",").join(dseConfig.getAuditLogCategories());
                            line = line.substring(0, line.indexOf("=") + 1).concat(cats);
                        }
                        else if(line.contains("ExemptKeyspaces"))
                        {
                            line = line.substring(0, line.indexOf("=") + 1).concat(dseConfig.getAuditLogExemptKeyspaces());
                        }
                    }
                    else
                    {
                        if(line.startsWith("#"))
                        {
                            //make sure there's only one # at the beginning of the line
                            while(line.charAt(1) == '#')
                                line = line.substring(1);
                        }
                        else
                        {
                            line = "#" + line;
                        }
                    }
                }
                writer.append(line);
                writer.newLine();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException("Unable to read " + AUDIT_LOG_FILE, e);
           
        }
        finally
        {
            FileUtils.closeQuietly(writer);
        }
    }

    private final String findAuditLoggerName(List<String> lines) throws IllegalStateException
    {
        for(final String l : lines)
        {
            if(l.contains(PRIMARY_AUDIT_LOG_ENTRY))
            {
                final String[] valTokens = l.split(",");
                return valTokens[valTokens.length -1].trim();
            }
        }
        throw new IllegalStateException();
    }

    protected String getSnitch()
    {
        return dseConfig.getDseDelegatingSnitch();
    }
}
