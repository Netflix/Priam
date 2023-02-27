package com.netflix.priam.tuner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.netflix.priam.config.IConfiguration;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support tuning standard .properties files
 *
 * <p>
 */
public class PropertiesFileTuner {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesFileTuner.class);
    protected final IConfiguration config;

    @Inject
    public PropertiesFileTuner(IConfiguration config) {
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    public void updateAndSaveProperties(String propertyFile)
            throws IOException, ConfigurationException {
        try {
            PropertiesConfiguration properties = new PropertiesConfiguration();
            properties.getLayout().load(properties, new FileReader(propertyFile));
            String overrides =
                    config.getProperty(
                            "propertyOverrides." + FilenameUtils.getBaseName(propertyFile), null);
            if (overrides != null) {
                // Allow use of the IConfiguration object as template strings
                Map<String, Object> map = new ObjectMapper().convertValue(config, Map.class);
                String resolvedOverrides = new StringSubstitutor(map).replace(overrides);
                Splitter.on(",")
                        .withKeyValueSeparator("=")
                        .split(resolvedOverrides)
                        .forEach(properties::setProperty);
            }
            properties.getLayout().save(properties, new FileWriter(propertyFile));
        } catch (IOException | ConfigurationException e) {
            logger.error("Could not tune " + propertyFile + ". Does it exist? Is it writable?", e);
            throw e;
        }
    }
}
