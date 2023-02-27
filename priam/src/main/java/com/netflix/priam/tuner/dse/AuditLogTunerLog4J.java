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

package com.netflix.priam.tuner.dse;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.netflix.priam.config.IConfiguration;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.Charset;
import java.util.List;
import javax.inject.Inject;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dse tuner for audit log via log4j. Use this instead of AuditLogTunerYaml if you are on DSE
 * version 3.x. Created by aagrawal on 8/8/17.
 */
public class AuditLogTunerLog4J implements IAuditLogTuner {

    private final IConfiguration config;
    private final IDseConfiguration dseConfig;
    protected static final String AUDIT_LOG_ADDITIVE_ENTRY = "log4j.additivity.DataAudit";
    protected static final String AUDIT_LOG_FILE = "/conf/log4j-server.properties";
    protected static final String PRIMARY_AUDIT_LOG_ENTRY = "log4j.logger.DataAudit";
    private static final Logger logger = LoggerFactory.getLogger(AuditLogTunerLog4J.class);

    @Inject
    public AuditLogTunerLog4J(IConfiguration config, IDseConfiguration dseConfig) {
        this.config = config;
        this.dseConfig = dseConfig;
    }

    /**
     * Note: supporting the direct hacking of a log4j props file is far from elegant, but seems less
     * odious than other solutions I've come up with. Operates under the assumption that the only
     * people mucking with the audit log entries in the value are DataStax themselves and this
     * program, and that the original property names are somehow still preserved. Otherwise, YMMV.
     */
    public void tuneAuditLog() {
        BufferedWriter writer = null;
        try {
            final File srcFile = new File(config.getCassHome() + AUDIT_LOG_FILE);
            final List<String> lines = Files.readLines(srcFile, Charset.defaultCharset());
            final File backupFile =
                    new File(
                            config.getCassHome()
                                    + AUDIT_LOG_FILE
                                    + "."
                                    + System.currentTimeMillis());
            Files.move(srcFile, backupFile);
            writer = Files.newWriter(srcFile, Charset.defaultCharset());

            String loggerPrefix = "log4j.appender.";
            try {
                loggerPrefix += findAuditLoggerName(lines);
            } catch (IllegalStateException ise) {
                logger.warn(
                        "cannot locate "
                                + PRIMARY_AUDIT_LOG_ENTRY
                                + " property, will ignore any audit log updating");
                return;
            }

            for (String line : lines) {
                if (line.contains(loggerPrefix)
                        || line.contains(PRIMARY_AUDIT_LOG_ENTRY)
                        || line.contains(AUDIT_LOG_ADDITIVE_ENTRY)) {
                    if (dseConfig.isAuditLogEnabled()) {
                        // first, check to see if we need to uncomment the line
                        while (line.startsWith("#")) {
                            line = line.substring(1);
                        }

                        // next, check if we need to change the prop's value
                        if (line.contains("ActiveCategories")) {
                            final String cats =
                                    Joiner.on(",").join(dseConfig.getAuditLogCategories());
                            line = line.substring(0, line.indexOf("=") + 1).concat(cats);
                        } else if (line.contains("ExemptKeyspaces")) {
                            line =
                                    line.substring(0, line.indexOf("=") + 1)
                                            .concat(dseConfig.getAuditLogExemptKeyspaces());
                        }
                    } else {
                        if (line.startsWith("#")) {
                            // make sure there's only one # at the beginning of the line
                            while (line.charAt(1) == '#') line = line.substring(1);
                        } else {
                            line = "#" + line;
                        }
                    }
                }
                writer.append(line);
                writer.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to read " + AUDIT_LOG_FILE, e);

        } finally {
            FileUtils.closeQuietly(writer);
        }
    }

    private String findAuditLoggerName(List<String> lines) throws IllegalStateException {
        for (final String l : lines) {
            if (l.contains(PRIMARY_AUDIT_LOG_ENTRY)) {
                final String[] valTokens = l.split(",");
                return valTokens[valTokens.length - 1].trim();
            }
        }
        throw new IllegalStateException();
    }
}
