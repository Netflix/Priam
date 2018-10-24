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

package com.netflix.priam.backupv2;

import com.google.inject.Inject;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to create a local DB entry for SSTable components. This will be used to
 * identify when a version of a SSTable component was last uploaded or last referenced. This db
 * entry will be used to enforce the TTL of a version of a SSTable component as we will not be
 * relying on backup file system level TTL. Local DB should be copied over to new instance replacing
 * this token in case of instance replacement. If no local DB is found then Priam will try to
 * re-create the local DB using meta files uploaded to backup file system. The check operation for
 * local DB is done at every start of Priam and when operator requests to re-build the local DB.
 * Created by aagrawal on 8/31/18.
 */
public class LocalDBReaderWriter {
    private static final Logger logger = LoggerFactory.getLogger(LocalDBReaderWriter.class);
    private final IConfiguration configuration;
    public static final String LOCAL_DB = "localdb";

    @Inject
    public LocalDBReaderWriter(IConfiguration configuration) {
        this.configuration = configuration;
    }

    public synchronized LocalDB upsertLocalDBEntry(final LocalDBEntry localDBEntry)
            throws Exception {
        // validate the localDBEntry first
        if (localDBEntry.getTimeLastReferenced() == null)
            throw new Exception("Time last referenced in localDB can never be null");

        if (localDBEntry.getBackupTime() == null)
            throw new Exception("Backup time for localDB can never be null");

        final Path localDBFile = getLocalDBPath(localDBEntry.getFileUploadResult());

        localDBFile.getParent().toFile().mkdirs();

        LocalDB localDB = readAndGetLocalDB(localDBEntry.getFileUploadResult());

        if (localDB == null) localDB = new LocalDB(new ArrayList<>());

        // Verify again if someone beat you to write the entry.
        LocalDBEntry entry = getLocalDBEntry(localDBEntry.getFileUploadResult(), localDB);
        // Write entry as it might be either -
        // 1. new component entry
        // 2. new version of file (change in compression type or file is modified e.g. stats file)
        if (entry == null) {
            localDB.getLocalDBEntries().add(localDBEntry);
            writeLocalDB(localDBFile, localDB);
        } else {
            // An entry already exists. Maybe last referenced time or backup time changed. We want
            // to write the last time referenced.
            entry.setBackupTime(localDBEntry.getBackupTime());
            entry.setTimeLastReferenced(localDBEntry.getTimeLastReferenced());
            writeLocalDB(localDBFile, localDB);
        }

        return localDB;
    }

    private LocalDB readAndGetLocalDB(final FileUploadResult fileUploadResult) throws Exception {
        final Path localDbPath = getLocalDBPath(fileUploadResult);
        return readLocalDB(localDbPath);
    }

    /**
     * Get the local database entry for a given file upload result.
     *
     * @param fileUploadResult File upload result for which local db is required.
     * @return LocalDBEntry if one exists.
     * @throws Exception if there is any error in getting local db file.
     */
    public LocalDBEntry getLocalDBEntry(final FileUploadResult fileUploadResult) throws Exception {
        LocalDB localDB = readAndGetLocalDB(fileUploadResult);
        return getLocalDBEntry(fileUploadResult, localDB);
    }

    private LocalDBEntry getLocalDBEntry(
            final FileUploadResult fileUploadResult, final LocalDB localDB) throws Exception {
        if (localDB == null
                || localDB.getLocalDBEntries() == null
                || localDB.getLocalDBEntries().isEmpty()) return null;

        // Get local db entry for same file and version.
        List<LocalDBEntry> localDBEntries =
                localDB.getLocalDBEntries()
                        .stream()
                        .filter(
                                localDBEntry ->
                                        // Name of the file should be same.
                                        (localDBEntry
                                                .getFileUploadResult()
                                                .getFileName()
                                                .toFile()
                                                .getName()
                                                .toLowerCase()
                                                .equals(
                                                        fileUploadResult
                                                                .getFileName()
                                                                .toFile()
                                                                .getName()
                                                                .toLowerCase())))
                        // Should be same version (same last modified time)
                        .filter(
                                localDBEntry ->
                                        (localDBEntry
                                                .getFileUploadResult()
                                                .getLastModifiedTime()
                                                .equals(fileUploadResult.getLastModifiedTime())))
                        // Same compression as before. If we switch compression technique we can
                        // upload again.
                        .filter(
                                localDBEntry ->
                                        (localDBEntry
                                                .getFileUploadResult()
                                                .getCompression()
                                                .equals(fileUploadResult.getCompression())))
                        .collect(Collectors.toList());

        if (localDBEntries.isEmpty()) return null;

        if (localDBEntries.size() == 1) {
            if (logger.isDebugEnabled())
                logger.debug("Local entry found: {}", localDBEntries.get(0));

            return localDBEntries.get(0);
        }

        throw new Exception(
                "Unexpected behavior: More than one entry found in local database for the same file. FileUploadResult: "
                        + fileUploadResult);
    }

    /**
     * Gets the local db path on the local file system.
     *
     * @param fileUploadResult This contains the SSTable component for which local db path is
     *     required.
     * @return the local db path on local file system.
     */
    public Path getLocalDBPath(final FileUploadResult fileUploadResult) {
        return Paths.get(
                configuration.getDataFileLocation(),
                LOCAL_DB,
                fileUploadResult.getKeyspaceName(),
                fileUploadResult.getColumnFamilyName(),
                PrefixGenerator.getSSTFileBase(fileUploadResult.getFileName().toFile().getName())
                        + ".localdb");
    }

    /**
     * Writes the local database to the local db file. This will do a complete replace of existing
     * local database if any.
     *
     * @param localDBFile path to the local database file.
     * @param localDB local database containing all the entries to the local database.
     * @throws Exception If the path denoted is directory, write permission issues or any other
     *     exceptions.
     */
    public void writeLocalDB(final Path localDBFile, final LocalDB localDB) throws Exception {
        if (localDB == null || localDBFile == null || localDBFile.toFile().isDirectory())
            throw new Exception(
                    "Invalid Arguments: localDbFile: " + localDBFile + ", localDB: " + localDB);

        if (!localDBFile.getParent().toFile().exists()) localDBFile.getParent().toFile().mkdirs();

        File tmpFile =
                File.createTempFile(
                        localDBFile.toFile().getName(), ".tmp", localDBFile.getParent().toFile());
        try (FileWriter writer = new FileWriter(tmpFile)) {
            writer.write(localDB.toString());

            // Atomically swap out the new local db file for the old local db file.
            if (!tmpFile.renameTo(localDBFile.toFile()))
                logger.error("Failed to persist local db: {}", localDB);
        } finally {
            if (tmpFile != null) Files.deleteIfExists(tmpFile.toPath());
        }
    }

    /**
     * Reads the local database file stored on local file system.
     *
     * @param localDBFile path the local database.
     * @return local database if file exists or empty local database.
     * @throws Exception If there is any error in de-serializing the object or any other file system
     *     errors.
     */
    public LocalDB readLocalDB(final Path localDBFile) throws Exception {
        // Verify file exists
        if (!localDBFile.toFile().exists()) return new LocalDB(new ArrayList<>());

        try (FileReader reader = new FileReader(localDBFile.toFile())) {
            LocalDB localDB = GsonJsonSerializer.getGson().fromJson(reader, LocalDB.class);
            String columnfamilyName = localDBFile.getParent().toFile().getName();
            String keyspaceName = localDBFile.getParent().getParent().toFile().getName();
            localDB.getLocalDBEntries()
                    .forEach(
                            localDBEntry -> {
                                localDBEntry
                                        .getFileUploadResult()
                                        .setColumnFamilyName(columnfamilyName);
                                localDBEntry.getFileUploadResult().setKeyspaceName(keyspaceName);
                            });

            if (logger.isDebugEnabled()) logger.debug("Local DB: {}", localDB);
            return localDB;
        }
    }

    static class LocalDB {
        private final List<LocalDBEntry> localDBEntries;

        public LocalDB(List<LocalDBEntry> localDBEntries) {
            this.localDBEntries = localDBEntries;
        }

        public List<LocalDBEntry> getLocalDBEntries() {

            return localDBEntries;
        }

        @Override
        public String toString() {
            return GsonJsonSerializer.getGson().toJson(this);
        }
    }

    static class LocalDBEntry {
        private final FileUploadResult fileUploadResult;
        private Instant timeLastReferenced;
        private Instant backupTime;

        public LocalDBEntry(FileUploadResult fileUploadResult) {
            this.fileUploadResult = fileUploadResult;
        }

        public FileUploadResult getFileUploadResult() {
            return fileUploadResult;
        }

        public Instant getTimeLastReferenced() {
            return timeLastReferenced;
        }

        public void setTimeLastReferenced(Instant timeLastReferenced) {
            this.timeLastReferenced = timeLastReferenced;
        }

        public Instant getBackupTime() {
            return backupTime;
        }

        public void setBackupTime(Instant backupTime) {
            this.backupTime = backupTime;
        }

        public LocalDBEntry(
                FileUploadResult fileUploadResult, Instant timeLastReferenced, Instant backupTime) {

            this.fileUploadResult = fileUploadResult;
            this.timeLastReferenced = timeLastReferenced;
            this.backupTime = backupTime;
        }

        @Override
        public String toString() {
            return GsonJsonSerializer.getGson().toJson(this);
        }
    }
}
