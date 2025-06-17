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

package com.netflix.priam.backupv2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

@Singleton
public class BackupDeleter {
    public static final String SUCCESS = "SUCCESS";
    private final BlobStoreBucket bucket;
    private final IMetaFileReader metaFileReader;
    private final BackupDeleterConfiguration configuration;
    private final DeletionQueue queue;
    private final Clock clock;
    private final Thrower thrower;

    @Inject
    public BackupDeleter(
            BlobStoreBucket bucket,
            IMetaFileReader metaFileReader,
            BackupDeleterConfiguration configuration,
            DeletionQueue queue,
            Clock clock,
            ThrowerFactory throwerFactory) {
        this.bucket = bucket;
        this.metaFileReader = metaFileReader;
        this.queue = queue;
        this.configuration = configuration;
        this.clock = clock;
        this.thrower = throwerFactory.create(LoggerFactory.getLogger(BackupDeleter.class));
    }

    public String deleteAll(String startAfter) {
        String appPrefix = startAfter;
        Instant dateToTtl = clock.instant().minus(configuration.getRetentionDays(), ChronoUnit.DAYS);
        try {
            String commonPrefix = BackupPaths.getCommonPrefix();
            Iterator<String> appPrefixes = getCommonPrefixes(commonPrefix, startAfter);
            while (appPrefixes.hasNext()) {
                appPrefix = getNextPrefix(appPrefixes, BackupPaths::validateAppPrefix, Failure.APP_PREFIX_IS_INVALID);
                Iterator<String> tokenPrefixes = getCommonPrefixes(appPrefix, startAfter);
                while (tokenPrefixes.hasNext()) {
                    String tokenPrefix = getNextPrefix(tokenPrefixes, BackupPaths::validateTokenPrefix, Failure.TOKEN_PREFIX_IS_INVALID);
                    deleteBackups(tokenPrefix, dateToTtl);
                    int orphanedFiles = queue.reset();
                    if (orphanedFiles > 0) {
                        String msg = String.format("There were %d files left over", orphanedFiles);
                        thrower.log(Failure.FILES_LEFT_OVER, msg);
                    }
                }
            }
        } catch (Exception e) {
            String errorMessage = String.format("Failed deleting backups for %s", appPrefix);
            thrower.log(Failure.GENERAL_FAILURE, errorMessage);
            return appPrefix;
        }
        return SUCCESS;
    }

    private String getNextPrefix(
            Iterator<String> prefixes,
            Function<String, Optional<String>> validator,
            Failure failure) throws BackupDeletionException {
        String prefix = prefixes.next();
        Optional<String> errorMessage = validator.apply(prefix);
        if (errorMessage.isPresent()) {
            thrower.abort(failure, errorMessage.get());
        }
        return prefix;
    }

    private Iterator<String> getCommonPrefixes(String prefix, String startAfter) throws BackupDeletionException {
        Iterator<String> commonPrefixes = bucket.commonPrefixes(prefix, BackupPaths.SEPARATOR, startAfter);
        if (!commonPrefixes.hasNext()) {
            String msg = String.format("No app prefixes were found common to %s", prefix);
            thrower.abort(Failure.NO_PREFIXES_FOUND, msg);
        }
        return commonPrefixes;
    }

    private void deleteBackups(String tokenPrefix, Instant dateToTtl) throws Exception {
        ImmutableList<String> metas = metaFileReader.getMetas(tokenPrefix, dateToTtl);
        for (int i = 0; i < metas.size() - 1; i++) {
            queue.add(metas.get(i));
        }
        ImmutableSet<String> filesToKeep = metaFileReader.read(metas.get(metas.size() - 1));
        Iterator<String> files = bucket.list(BackupPaths.JOINER.join(tokenPrefix, FileType.SST_V2));
        while (files.hasNext()) {
            String file = files.next();
            if (BackupPaths.getLastModified(file).compareTo(dateToTtl) >= 0) {
                break;
            }
            if (!filesToKeep.contains(BackupPaths.removeCompressionPart(file))) {
                queue.add(file);
            }
        }
        queue.flush();
    }
}
