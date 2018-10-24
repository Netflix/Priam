/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.config;

import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task that persists structured and merged priam configuration to disk. */
@Singleton
public class PriamConfigurationPersister extends Task {
    public static final String NAME = "PriamConfigurationPersister";

    private static final Logger logger = LoggerFactory.getLogger(PriamConfigurationPersister.class);

    private final Path mergedConfigDirectory;
    private final Path structuredPath;

    @Inject
    public PriamConfigurationPersister(IConfiguration config) {
        super(config);

        mergedConfigDirectory = Paths.get(config.getMergedConfigurationDirectory());
        structuredPath = Paths.get(config.getMergedConfigurationDirectory(), "structured.json");
    }

    private synchronized void ensurePaths() throws IOException {
        File directory = mergedConfigDirectory.toFile();

        if (directory.mkdirs()) {
            Files.setPosixFilePermissions(
                    mergedConfigDirectory, PosixFilePermissions.fromString("rwx------"));
            logger.info("Set up PriamConfigurationPersister directory successfully");
        }
    }

    @Override
    public void execute() throws Exception {
        ensurePaths();
        Path tempPath = null;
        try {
            File output =
                    File.createTempFile(
                            structuredPath.getFileName().toString(),
                            ".tmp",
                            mergedConfigDirectory.toFile());
            tempPath = output.toPath();

            // The configuration might contain sensitive information, so ... don't let non Priam
            // users read it
            // Theoretically createTempFile creates the file with the right permissions, but I want
            // to be explicit
            Files.setPosixFilePermissions(tempPath, PosixFilePermissions.fromString("rw-------"));

            Map<String, Object> structuredConfiguration = config.getStructuredConfiguration("all");

            ObjectMapper mapper = new ObjectMapper();
            ObjectWriter structuredPathTmpWriter = mapper.writer(new MinimalPrettyPrinter());
            structuredPathTmpWriter.writeValue(output, structuredConfiguration);

            // Atomically swap out the new config for the old config.
            if (!output.renameTo(structuredPath.toFile()))
                logger.error("Failed to persist structured Priam configuration");
        } finally {
            if (tempPath != null) Files.deleteIfExists(tempPath);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Timer to be used for configuration writing.
     *
     * @param config {@link IConfiguration} to get configuration details from priam.
     * @return the timer to be used for Configuration Persisting from {@link
     *     IConfiguration#getMergedConfigurationCronExpression()}
     */
    public static TaskTimer getTimer(IConfiguration config) {
        return CronTimer.getCronTimer(NAME, config.getMergedConfigurationCronExpression());
    }
}
