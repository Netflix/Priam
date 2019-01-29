/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.priam.backup;

import com.netflix.priam.scheduler.UnsupportedTypeException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enum to capture backup versions. Possible version are V1 and V2. Created by aagrawal on 1/29/19.
 */
public enum BackupVersion {
    SNAPSHOT_BACKUP(1),
    SNAPSHOT_META_SERVICE(2);

    private static final Logger logger = LoggerFactory.getLogger(BackupVersion.class);

    private final int backupVersion;
    private static Map<Integer, BackupVersion> map = new HashMap<>();

    static {
        for (BackupVersion backupVersion : BackupVersion.values()) {
            map.put(backupVersion.getBackupVersion(), backupVersion);
        }
    }

    BackupVersion(int backupVersion) {
        this.backupVersion = backupVersion;
    }

    public static BackupVersion lookup(int backupVersion, boolean acceptIllegalValue)
            throws UnsupportedTypeException {
        BackupVersion backupVersionResolved = map.get(backupVersion);
        if (backupVersionResolved == null) {
            String message =
                    String.format(
                            "%s is not a supported BackupVersion. Supported values are %s",
                            backupVersion, getSupportedValues());

            if (acceptIllegalValue) {
                message =
                        message
                                + ". Since acceptIllegalValue is set to True, returning NULL instead.";
                logger.error(message);
                return null;
            }

            logger.error(message);
            throw new UnsupportedTypeException(message);
        }
        return backupVersionResolved;
    }

    private static String getSupportedValues() {
        StringBuilder supportedValues = new StringBuilder();
        boolean first = true;
        for (BackupVersion type : BackupVersion.values()) {
            if (!first) {
                supportedValues.append(",");
            }
            supportedValues.append(type);
            first = false;
        }

        return supportedValues.toString();
    }

    public static BackupVersion lookup(int backupVersion) throws UnsupportedTypeException {
        return lookup(backupVersion, false);
    }

    public int getBackupVersion() {
        return backupVersion;
    }
}
