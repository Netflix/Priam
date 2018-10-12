/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.restore;

import com.google.inject.Inject;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * At run-time, determine the source type to restore from.
 */
public class RestoreContext {
    private final PriamScheduler scheduler;
    private final IConfiguration config;
    private static final Logger logger = LoggerFactory.getLogger(RestoreContext.class);

    @Inject
    public RestoreContext(IConfiguration config, PriamScheduler scheduler) {
        this.config = config;
        this.scheduler = scheduler;
    }

    public boolean isRestoreEnabled() {
        return !StringUtils.isEmpty(config.getRestoreSnapshot());
    }

    public void restore() throws Exception {
        if (!isRestoreEnabled()) return;

        // Restore is required.
        if (StringUtils.isEmpty(config.getRestoreSourceType()) && !config.isRestoreEncrypted()) {
            // Restore is needed and it will be done from the primary AWS account
            scheduler.addTask(
                    Restore.JOBNAME,
                    Restore.class,
                    Restore.getTimer()); // restore from the AWS primary acct
            logger.info("Scheduled task " + Restore.JOBNAME);
        } else if (config.isRestoreEncrypted()) {
            SourceType sourceType = SourceType.lookup(config.getRestoreSourceType(), true, false);

            if (sourceType == null) {
                scheduler.addTask(
                        EncryptedRestoreStrategy.JOBNAME,
                        EncryptedRestoreStrategy.class,
                        EncryptedRestoreStrategy.getTimer());
                logger.info("Scheduled task " + Restore.JOBNAME);
                return;
            }

            switch (sourceType) {
                case AWSCROSSACCT:
                    scheduler.addTask(
                            AwsCrossAccountCryptographyRestoreStrategy.JOBNAME,
                            AwsCrossAccountCryptographyRestoreStrategy.class,
                            AwsCrossAccountCryptographyRestoreStrategy.getTimer());
                    logger.info(
                            "Scheduled task " + AwsCrossAccountCryptographyRestoreStrategy.JOBNAME);
                    break;

                case GOOGLE:
                    scheduler.addTask(
                            GoogleCryptographyRestoreStrategy.JOBNAME,
                            GoogleCryptographyRestoreStrategy.class,
                            GoogleCryptographyRestoreStrategy.getTimer());
                    logger.info("Scheduled task " + GoogleCryptographyRestoreStrategy.JOBNAME);
                    break;
            }
        }
    }

    enum SourceType {
        AWSCROSSACCT("AWSCROSSACCT"),
        GOOGLE("GOOGLE");

        private static final Logger logger = LoggerFactory.getLogger(SourceType.class);

        private final String sourceType;

        SourceType(String sourceType) {
            this.sourceType = sourceType.toUpperCase();
        }

        public static SourceType lookup(
                String sourceType, boolean acceptNullOrEmpty, boolean acceptIllegalValue)
                throws UnsupportedTypeException {
            if (StringUtils.isEmpty(sourceType))
                if (acceptNullOrEmpty) return null;
                else {
                    String message =
                            String.format(
                                    "%s is not a supported SourceType. Supported values are %s",
                                    sourceType, getSupportedValues());
                    logger.error(message);
                    throw new UnsupportedTypeException(message);
                }

            try {
                return SourceType.valueOf(sourceType.toUpperCase());
            } catch (IllegalArgumentException ex) {
                String message =
                        String.format(
                                "%s is not a supported SourceType. Supported values are %s",
                                sourceType, getSupportedValues());

                if (acceptIllegalValue) {
                    message =
                            message
                                    + ". Since acceptIllegalValue is set to True, returning NULL instead.";
                    logger.error(message);
                    return null;
                }

                logger.error(message);
                throw new UnsupportedTypeException(message, ex);
            }
        }

        private static String getSupportedValues() {
            StringBuilder supportedValues = new StringBuilder();
            boolean first = true;
            for (SourceType type : SourceType.values()) {
                if (!first) supportedValues.append(",");
                supportedValues.append(type);
                first = false;
            }

            return supportedValues.toString();
        }

        public static SourceType lookup(String sourceType) throws UnsupportedTypeException {
            return lookup(sourceType, false, false);
        }

        public String getSourceType() {
            return sourceType;
        }
    }
}
