package com.netflix.priam.tuner;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.GsonJsonSerializer;

/**
 * Support tuning standard .properties files
 * <p>
 */
public class PropertiesFileTuner
{
    private static final Logger logger = LoggerFactory.getLogger(JVMOptionsTuner.class);
    protected final IConfiguration config;
    protected final String propertyPrefix;

    @Inject
    public PropertiesFileTuner(IConfiguration config, String prefix) {
        this.config = config;
        this.propertyPrefix = prefix;
    }

    @SuppressWarnings("unchecked")
    public void updateAndSaveProperties(String configPath) {
        File propertiesFile = new File(configPath);
        try {
            if (propertiesFile.exists() && !propertiesFile.canWrite()) {
                throw new IOException("Can't write and therefore cannot tune" + configPath);
            }

            PropertiesConfiguration properties = new PropertiesConfiguration();
            properties.getLayout().load(properties, new FileReader(propertiesFile.getPath()));

            Set<String> keys = new HashSet<>();
            properties.getKeys().forEachRemaining(keys::add);

            Map<String, String> overridenProperties = new HashMap<>();
            String propertyOverrides = config.getProperty("propertyOverrides." + propertyPrefix, null);

            if (propertyOverrides != null) {
                // Allow use of the IConfiguration object as template strings
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> map = objectMapper.convertValue(config, Map.class);
                StrSubstitutor sub = new StrSubstitutor(map);
                String resolvedPropertyOverrides = sub.replace(propertyOverrides);

                String[] pairs = resolvedPropertyOverrides.split(",");
                for (String kv : pairs) {
                    String[] entry = kv.split("=");
                    if (entry.length != 2)
                        continue;
                    keys.add(entry[0]);
                    overridenProperties.put(entry[0], entry[1]);
                }
            }

            for (String key: keys) {
                if (overridenProperties.containsKey(key))
                    properties.setProperty(key, overridenProperties.get(key));
            }

            properties.getLayout().save(properties, new FileWriter(propertiesFile.getPath()));
        }
        catch (IOException | ConfigurationException e) {
            logger.error("Could not tune " + configPath, e);
        }
    }

}
