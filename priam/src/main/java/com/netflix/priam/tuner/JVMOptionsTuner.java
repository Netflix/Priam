/*
 * Copyright 2016 Netflix, Inc.
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

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This is to tune the jvm.options file introduced in Cassandra 3.x to pass JVM parameters to Cassandra.
 * It supports configuring GC type (CMS/G1GC) where it automatically activates default properties as provided in
 * jvm.options file. Note that this will not "add" any GC options.
 * <p>
 * Created by aagrawal on 8/23/17.
 */
public class JVMOptionsTuner {
    private static final Logger logger = LoggerFactory.getLogger(JVMOptionsTuner.class);
    protected final IConfiguration config;

    @Inject
    public JVMOptionsTuner(IConfiguration config) {
        this.config = config;
    }

    /**
     * Update the JVM options file for cassandra by updating/removing JVM options {@link IConfiguration#getJVMExcludeSet()} && {@link IConfiguration#getJVMUpsertSet()}, configuring GC {@link IConfiguration#getGCType()}etc.
     *
     * @param outputFile File name with which this configured JVM options should be written.
     * @throws Exception when encountered with invalid configured GC type. {@link IConfiguration#getGCType()}
     */
    public void updateJVMOptions(final String outputFile) throws Exception {
        List<String> configuredJVMOptions = updateJVMOptions();

        //Verify we can write to output file and it is not directory.
        File file = new File(outputFile);
        if (file.exists() && !file.canWrite()) {
            throw new Exception("Not enough permissions to write to file: " + outputFile);
        }

        //Write jvm.options back to override defaults.
        Files.write(new File(outputFile).toPath(), configuredJVMOptions);
    }

    /**
     * Update the JVM options file for cassandra by updating/removing JVM options {@link IConfiguration#getJVMExcludeSet()} && {@link IConfiguration#getJVMUpsertSet()}, configuring GC {@link IConfiguration#getGCType()}etc.
     *
     * @return List of Configuration as String after reading the configuration from jvm.options
     * @throws Exception when encountered with invalid configured GC type. {@link IConfiguration#getGCType()}
     */
    protected List<String> updateJVMOptions() throws Exception {
        File jvmOptionsFile = new File(config.getJVMOptionsFileLocation());
        validate(jvmOptionsFile);
        final GCType configuredGC = config.getGCType();

        //Read file line by line.
        final Map<String, JVMOption> excludeSet = config.getJVMExcludeSet();

        //Make a copy of upsertSet, so we can delete the entries as we process them.
        Map<String, JVMOption> upsertSet = config.getJVMUpsertSet();

        //Don't use streams for processing as upsertSet jvm options needs to be removed if we find them
        //already in jvm.options file.
        List<String> optionsFromFile = Files.lines(jvmOptionsFile.toPath()).collect(Collectors.toList());
        List<String> configuredOptions = new LinkedList<>();
        for (String line: optionsFromFile)
        {
            configuredOptions.add(updateConfigurationValue(line, configuredGC, upsertSet, excludeSet));
        }

        //Add all the upserts(inserts only left) from config.
        if (upsertSet != null && !upsertSet.isEmpty()) {

            configuredOptions.add("#################");
            configuredOptions.add("# USER PROVIDED CUSTOM JVM CONFIGURATIONS #");
            configuredOptions.add("#################");

            configuredOptions.addAll(upsertSet.values().stream()
                    .map(jvmOption -> jvmOption.toString()).collect(Collectors.toList()));
        }

        return configuredOptions;
    }

    private String updateConfigurationValue(final String line, GCType configuredGC, Map<String, JVMOption> upsertSet, Map<String, JVMOption> excludeSet) {

        JVMOption option = JVMOption.parse(line);
        if (option == null)
            return line;

        //Is parameter for GC.
        GCType gcType = GCTuner.isOptionAvailable(option);
        if (gcType != null) {
            option.setCommented(gcType != configuredGC);
        }

        //See if option is in upsert list.
        if (upsertSet != null && upsertSet.containsKey(option.getJvmOption())) {
            JVMOption configuration = upsertSet.get(option.getJvmOption());
            option.setCommented(false);
            option.setValue(configuration.getValue());
            upsertSet.remove(option.getJvmOption());
        }
        ;

        //See if option is in exclude list.
        if (excludeSet != null && excludeSet.containsKey(option.getJvmOption()))
            option.setCommented(true);

        return option.toString();
    }

    private void validate(File jvmOptionsFile) throws Exception {
        if (!jvmOptionsFile.exists())
            throw new Exception("JVM Option File does not exist: " + jvmOptionsFile.getAbsolutePath());

        if (jvmOptionsFile.isDirectory())
            throw new Exception("JVM Option File is a directory: " + jvmOptionsFile.getAbsolutePath());

        if (!jvmOptionsFile.canRead() || !jvmOptionsFile.canWrite())
            throw new Exception("JVM Option File does not have right permission: " + jvmOptionsFile.getAbsolutePath());

    }

}
