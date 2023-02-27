/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.tuner;

import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is to tune the jvm.options file introduced in Cassandra 3.x to pass JVM parameters to
 * Cassandra. It supports configuring GC type (CMS/G1GC) where it automatically activates default
 * properties as provided in jvm.options file. Note that this will not "add" any GC options.
 *
 * <p>Created by aagrawal on 8/23/17.
 */
public class JVMOptionsTuner {
    private static final Logger logger = LoggerFactory.getLogger(JVMOptionsTuner.class);
    protected final IConfiguration config;

    @Inject
    public JVMOptionsTuner(IConfiguration config) {
        this.config = config;
    }

    /**
     * Update the JVM options file and save to a file for cassandra by updating/removing JVM options
     * {@link IConfiguration#getJVMExcludeSet()} and {@link IConfiguration#getJVMUpsertSet()},
     * configuring GC {@link IConfiguration#getGCType()}etc.
     *
     * @param outputFile File name with which this configured JVM options should be written.
     * @throws Exception when encountered with invalid configured GC type. {@link
     *     IConfiguration#getGCType()}
     */
    public void updateAndSaveJVMOptions(final String outputFile) throws Exception {
        List<String> configuredJVMOptions = updateJVMOptions();

        if (logger.isInfoEnabled()) {
            StringBuffer buffer = new StringBuffer("\n");
            configuredJVMOptions.stream().forEach(line -> buffer.append(line).append("\n"));
            logger.info("Updating jvm.options with following values: " + buffer.toString());
        }

        // Verify we can write to output file and it is not directory.
        File file = new File(outputFile);
        if (file.exists() && !file.canWrite()) {
            throw new Exception("Not enough permissions to write to file: " + outputFile);
        }

        // Write jvm.options back to override defaults.
        Files.write(new File(outputFile).toPath(), configuredJVMOptions);
    }

    /**
     * Update the JVM options file for cassandra by updating/removing JVM options {@link
     * IConfiguration#getJVMExcludeSet()} and {@link IConfiguration#getJVMUpsertSet()}, configuring
     * GC {@link IConfiguration#getGCType()}etc.
     *
     * @return List of Configuration as String after reading the configuration from jvm.options
     * @throws Exception when encountered with invalid configured GC type. {@link
     *     IConfiguration#getGCType()}
     */
    protected List<String> updateJVMOptions() throws Exception {
        File jvmOptionsFile = new File(config.getJVMOptionsFileLocation());
        validate(jvmOptionsFile);
        final GCType configuredGC = config.getGCType();

        final Map<String, JVMOption> excludeSet =
                JVMOptionsTuner.parseJVMOptions(config.getJVMExcludeSet());

        // Make a copy of upsertSet, so we can delete the entries as we process them.
        Map<String, JVMOption> upsertSet =
                JVMOptionsTuner.parseJVMOptions(config.getJVMUpsertSet());

        // Don't use streams for processing as upsertSet jvm options needs to be removed if we find
        // them
        // already in jvm.options file.
        List<String> optionsFromFile =
                Files.lines(jvmOptionsFile.toPath()).collect(Collectors.toList());
        List<String> configuredOptions = new LinkedList<>();
        for (String line : optionsFromFile) {
            configuredOptions.add(
                    updateConfigurationValue(line, configuredGC, upsertSet, excludeSet));
        }

        // Add all the upserts(inserts only left) from config.
        if (upsertSet != null && !upsertSet.isEmpty()) {

            configuredOptions.add("#################");
            configuredOptions.add("# USER PROVIDED CUSTOM JVM CONFIGURATIONS #");
            configuredOptions.add("#################");

            configuredOptions.addAll(
                    upsertSet
                            .values()
                            .stream()
                            .map(JVMOption::toJVMOptionString)
                            .collect(Collectors.toList()));
        }

        return configuredOptions;
    }

    private void setHeapSetting(String configuredValue, JVMOption option) {
        if (!StringUtils.isEmpty(configuredValue))
            option.setCommented(false).setValue(configuredValue);
    }

    /**
     * @param line a line as read from jvm.options file.
     * @param configuredGC GCType configured by user for Cassandra.
     * @param upsertSet configured upsert set of JVM properties as provided by user for Cassandra.
     * @param excludeSet configured exclude set of JVM properties as provided by user for Cassandra.
     * @return the "comment" as is, if not a valid JVM option. Else, a string representation of JVM
     *     option
     */
    private String updateConfigurationValue(
            final String line,
            GCType configuredGC,
            Map<String, JVMOption> upsertSet,
            Map<String, JVMOption> excludeSet) {

        JVMOption option = JVMOption.parse(line);
        if (option == null) return line;

        // Is parameter for heap setting.
        if (option.isHeapJVMOption()) {
            String configuredValue;
            switch (option.getJvmOption()) {
                    // Special handling for heap new size ("Xmn")
                case "-Xmn":
                    configuredValue = config.getHeapNewSize();
                    break;
                    // Set min and max heap size to same value
                default:
                    configuredValue = config.getHeapSize();
                    break;
            }
            setHeapSetting(configuredValue, option);
        }

        // We don't want Xmn with G1GC, allow the GC to determine optimal young gen
        if (option.getJvmOption().equals("-Xmn") && configuredGC == GCType.G1GC)
            option.setCommented(true);

        // Is parameter for GC.
        GCType gcType = GCTuner.getGCType(option);
        if (gcType != null) {
            option.setCommented(gcType != configuredGC);
        }

        // See if option is in upsert list.
        if (upsertSet != null && upsertSet.containsKey(option.getJvmOption())) {
            JVMOption configuration = upsertSet.get(option.getJvmOption());
            option.setCommented(false);
            option.setValue(configuration.getValue());
            upsertSet.remove(option.getJvmOption());
        }

        // See if option is in exclude list.
        if (excludeSet != null && excludeSet.containsKey(option.getJvmOption()))
            option.setCommented(true);

        return option.toJVMOptionString();
    }

    private void validate(File jvmOptionsFile) throws Exception {
        if (!jvmOptionsFile.exists())
            throw new Exception(
                    "JVM Option File does not exist: " + jvmOptionsFile.getAbsolutePath());

        if (jvmOptionsFile.isDirectory())
            throw new Exception(
                    "JVM Option File is a directory: " + jvmOptionsFile.getAbsolutePath());

        if (!jvmOptionsFile.canRead() || !jvmOptionsFile.canWrite())
            throw new Exception(
                    "JVM Option File does not have right permission: "
                            + jvmOptionsFile.getAbsolutePath());
    }

    /**
     * Util function to parse comma separated list of jvm options to a Map (jvmOptionName,
     * JVMOption). It will ignore anything which is not a valid JVM option.
     *
     * @param property comma separated list of JVM options.
     * @return Map of (jvmOptionName, JVMOption).
     */
    public static final Map<String, JVMOption> parseJVMOptions(String property) {
        if (StringUtils.isEmpty(property)) return null;
        return new HashSet<>(Arrays.asList(property.split(",")))
                .stream()
                .map(JVMOption::parse)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(JVMOption::getJvmOption, jvmOption -> jvmOption));
    }
}
