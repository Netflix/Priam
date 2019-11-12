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
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * At run-time, determine the source type to restore from.
 */
public class RestoreContext {
    private final IConfiguration config;
    private final IRestoreStrategy restore;
    private final IRestoreStrategy encryptedRestoreStrategy;
    private final IRestoreStrategy awsCrossAccountCryptographyRestoreStrategy;
    private final IRestoreStrategy googleCryptographyRestoreStrategy;
    private static final Logger logger = LoggerFactory.getLogger(RestoreContext.class);

    @Inject
    public RestoreContext(
            IConfiguration config,
            Restore restore,
            EncryptedRestoreStrategy encryptedRestoreStrategy,
            AwsCrossAccountCryptographyRestoreStrategy awsCrossAccountCryptographyRestoreStrategy,
            GoogleCryptographyRestoreStrategy googleCryptographyRestoreStrategy) {
        this.config = config;
        this.restore = restore;
        this.awsCrossAccountCryptographyRestoreStrategy =
                awsCrossAccountCryptographyRestoreStrategy;
        this.encryptedRestoreStrategy = encryptedRestoreStrategy;
        this.googleCryptographyRestoreStrategy = googleCryptographyRestoreStrategy;
    }

    /**
     * Find if restore is required based on input set.
     *
     * @param conf configuration parameters.
     * @param instanceInfo InstanceInfo to find if backup was enabled on this rac.
     * @return boolean value indicating if restore is feasible/enabled.
     */
    public static boolean isRestoreEnabled(IConfiguration conf, InstanceInfo instanceInfo) {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getRestoreSnapshot());
        boolean isBackedupRac =
                (CollectionUtils.isEmpty(conf.getBackupRacs())
                        || conf.getBackupRacs().contains(instanceInfo.getRac()));
        return (isRestoreMode && isBackedupRac);
    }

    /**
     * Perform the restore based on the configuration provided. Note that this method do not check
     * if restore is required or not. We leave that decision to the caller.
     *
     * @throws Exception
     */
    public void restore() throws Exception {
        IRestoreStrategy restoreStrategy = getRestoreStrategy();
        if (restoreStrategy != null) {
            logger.info("Restore using {}", restoreStrategy.getClass());
            restoreStrategy.restore();
        }
    }

    private IRestoreStrategy getRestoreStrategy() throws Exception {
        if (StringUtils.isEmpty(config.getRestoreSourceType()) && !config.isRestoreEncrypted()) {
            // Restore is needed and it will be done from the primary AWS account
            return restore;
        } else if (config.isRestoreEncrypted()) {
            SourceType sourceType = SourceType.lookup(config.getRestoreSourceType(), true, false);

            if (sourceType == null) {
                return encryptedRestoreStrategy;
            }

            switch (sourceType) {
                case AWSCROSSACCT:
                    return awsCrossAccountCryptographyRestoreStrategy;

                case GOOGLE:
                    return googleCryptographyRestoreStrategy;
            }
        }
        return null;
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
