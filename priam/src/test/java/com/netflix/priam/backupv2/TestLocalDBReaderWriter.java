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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.cassandra.io.sstable.Component;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 9/4/18. */
public class TestLocalDBReaderWriter {
    private static final Logger logger =
            LoggerFactory.getLogger(TestLocalDBReaderWriter.class.getName());
    private static IConfiguration configuration;
    private static LocalDBReaderWriter localDBReaderWriter;
    private static Path dummyDataDirectoryLocation;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new BRTestModule());

        if (configuration == null) configuration = injector.getInstance(IConfiguration.class);

        if (localDBReaderWriter == null)
            localDBReaderWriter = injector.getInstance(LocalDBReaderWriter.class);

        dummyDataDirectoryLocation = Paths.get(configuration.getDataFileLocation());
        cleanupDir(dummyDataDirectoryLocation);
    }

    @After
    public void destroy() {
        cleanupDir(dummyDataDirectoryLocation);
    }

    private void cleanupDir(Path dir) {
        if (dir.toFile().exists())
            try {
                FileUtils.cleanDirectory(dir.toFile());
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Test
    public void readWriteLocalDB() throws Exception {
        int noOfKeyspaces = 2;
        int noOfCf = 1;
        int noOfSstables = 2;
        Path localDbPath =
                Paths.get(dummyDataDirectoryLocation.toString(), LocalDBReaderWriter.LOCAL_DB);
        List<LocalDBReaderWriter.LocalDB> localDBList =
                generateDummyLocalDB(noOfKeyspaces, noOfCf, noOfSstables);

        localDBList.forEach(
                localDB -> {
                    FileUploadResult fileUploadResult =
                            localDB.getLocalDBEntries().get(0).getFileUploadResult();
                    final Path localDBPath = localDBReaderWriter.getLocalDBPath(fileUploadResult);
                    try {
                        localDBReaderWriter.writeLocalDB(localDBPath, localDB);
                    } catch (Exception e) {
                        logger.error("Error while writing to local DB: " + e.getMessage(), e);
                    }
                });

        // Verify the write succeeded for each KS/CF/SStable.
        Assert.assertEquals(localDbPath.toFile().listFiles().length, noOfKeyspaces);
        Path cfLocalDBPath = localDbPath.toFile().listFiles()[0].listFiles()[0].toPath();
        Assert.assertEquals(noOfSstables, cfLocalDBPath.toFile().listFiles().length);

        // Read the database.
        LocalDBReaderWriter.LocalDB localDB =
                localDBReaderWriter.readLocalDB(cfLocalDBPath.toFile().listFiles()[0].toPath());
        Assert.assertEquals(
                EnumSet.allOf(Component.Type.class).size(), localDB.getLocalDBEntries().size());
    }

    @Test
    public void upsertLocalDB() throws Exception {
        LocalDBReaderWriter.LocalDB localDB = generateDummyLocalDB(1, 1, 1).get(0);

        // Lets do write with each LocalDBEntry first.
        localDB.getLocalDBEntries()
                .forEach(
                        localDBEntry -> {
                            try {
                                localDBReaderWriter.upsertLocalDBEntry(localDBEntry);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });

        // Verify the write has happened.
        LocalDBReaderWriter.LocalDBEntry localDBEntry =
                localDBReaderWriter.getLocalDBEntry(
                        localDB.getLocalDBEntries().get(0).getFileUploadResult());
        Assert.assertNotNull(localDBEntry);

        // Now lets see if we can write the same entry again??
        LocalDBReaderWriter.LocalDB localDBUpsert =
                localDBReaderWriter.upsertLocalDBEntry(localDBEntry);
        Assert.assertEquals(
                localDB.getLocalDBEntries().size(), localDBUpsert.getLocalDBEntries().size());

        // Now lets change the localDBEntry and see if upsert succeeds.
        localDBEntry.setTimeLastReferenced(DateUtil.getInstant());
        localDBUpsert = localDBReaderWriter.upsertLocalDBEntry(localDBEntry);
        LocalDBReaderWriter.LocalDBEntry localDBEntryUpsert =
                localDBReaderWriter.getLocalDBEntry(localDBEntry.getFileUploadResult());
        Assert.assertEquals(
                localDBEntryUpsert.getTimeLastReferenced(), localDBEntry.getTimeLastReferenced());
        Assert.assertEquals(
                localDB.getLocalDBEntries().size(), localDBUpsert.getLocalDBEntries().size());

        // Now change the file modification time. This should end up creating a new DB Entry.
        localDBEntry.getFileUploadResult().setLastModifiedTime(DateUtil.getInstant());
        localDBUpsert = localDBReaderWriter.upsertLocalDBEntry(localDBEntry);
        localDBEntryUpsert =
                localDBReaderWriter.getLocalDBEntry(localDBEntry.getFileUploadResult());
        Assert.assertEquals(
                localDB.getLocalDBEntries().size() + 1, localDBUpsert.getLocalDBEntries().size());
        Assert.assertEquals(
                localDBEntry.getFileUploadResult().getLastModifiedTime(),
                localDBEntryUpsert.getFileUploadResult().getLastModifiedTime());
    }

    @Test
    public void readConcurrentLocalDB() throws Exception {
        List<LocalDBReaderWriter.LocalDB> localDBList = generateDummyLocalDB(1, 1, 1);
        localDBList.forEach(
                localDB -> {
                    FileUploadResult fileUploadResult =
                            localDB.getLocalDBEntries().get(0).getFileUploadResult();
                    final Path localDBPath = localDBReaderWriter.getLocalDBPath(fileUploadResult);
                    try {
                        localDBReaderWriter.writeLocalDB(localDBPath, localDB);
                    } catch (Exception e) {
                        logger.error("Error while writing to local DB: " + e.getMessage(), e);
                    }
                });

        FileUploadResult sample =
                localDBList.get(0).getLocalDBEntries().get(0).getFileUploadResult();
        int size = 5;

        ExecutorService threads = Executors.newFixedThreadPool(size);
        List<Callable<Boolean>> torun = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            torun.add(() -> localDBReaderWriter.getLocalDBEntry(sample) != null);
        }

        // all tasks executed in different threads, at 'once'.
        List<Future<Boolean>> futures = threads.invokeAll(torun);

        // no more need for the threadpool
        threads.shutdown();
        // check the results of the tasks.
        int noOfBadRun = 0;
        for (Future<Boolean> fut : futures) {
            if (!fut.get()) noOfBadRun++;
        }

        Assert.assertEquals(0, noOfBadRun);
    }

    @Test
    public void writeConcurrentLocalDB() throws Exception {
        LocalDBReaderWriter.LocalDB localDB = generateDummyLocalDB(1, 1, 1).get(0);
        int size = localDB.getLocalDBEntries().size();
        ExecutorService threads = Executors.newFixedThreadPool(size);
        List<Callable<LocalDBReaderWriter.LocalDB>> torun = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int finalI = i;
            torun.add(
                    () ->
                            localDBReaderWriter.upsertLocalDBEntry(
                                    localDB.getLocalDBEntries().get(finalI)));
        }
        // all tasks executed in different threads, at 'once'.
        List<Future<LocalDBReaderWriter.LocalDB>> futures = threads.invokeAll(torun);

        // no more need for the threadpool
        threads.shutdown();
        // check the results of the tasks.
        int noOfBadRun = 0;
        for (Future<LocalDBReaderWriter.LocalDB> fut : futures) {
            // We expect exception here.
            try {
                fut.get();
            } catch (Exception e) {
                noOfBadRun++;
            }
        }

        Assert.assertEquals(0, noOfBadRun);
        LocalDBReaderWriter.LocalDB localDBRead =
                localDBReaderWriter.readLocalDB(
                        localDBReaderWriter.getLocalDBPath(
                                localDB.getLocalDBEntries().get(0).getFileUploadResult()));
        Assert.assertEquals(
                localDB.getLocalDBEntries().size(), localDBRead.getLocalDBEntries().size());
    }

    private List<LocalDBReaderWriter.LocalDB> generateDummyLocalDB(
            int noOfKeyspaces, int noOfCf, int noOfSstables) {

        // Clean the dummy directory
        cleanupDir(dummyDataDirectoryLocation);
        List<LocalDBReaderWriter.LocalDB> localDBList = new ArrayList<>();

        Random random = new Random();

        for (int i = 1; i <= noOfKeyspaces; i++) {
            String keyspaceName = "sample" + i;

            for (int j = 1; j <= noOfCf; j++) {
                String columnfamilyname = "cf" + j;

                for (int k = 1; k <= noOfSstables; k++) {
                    String prefixName = "mc-" + k + "-big";
                    LocalDBReaderWriter.LocalDB localDB =
                            new LocalDBReaderWriter.LocalDB(new ArrayList<>());
                    localDBList.add(localDB);
                    for (Component.Type type : EnumSet.allOf(Component.Type.class)) {
                        Path componentPath =
                                Paths.get(
                                        dummyDataDirectoryLocation.toFile().getAbsolutePath(),
                                        keyspaceName,
                                        columnfamilyname,
                                        prefixName + "-" + type.name() + ".db");
                        FileUploadResult fileUploadResult =
                                new FileUploadResult(
                                        componentPath,
                                        keyspaceName,
                                        columnfamilyname,
                                        DateUtil.getInstant(),
                                        DateUtil.getInstant(),
                                        random.nextLong());
                        LocalDBReaderWriter.LocalDBEntry localDBEntry =
                                new LocalDBReaderWriter.LocalDBEntry(
                                        fileUploadResult,
                                        DateUtil.getInstant(),
                                        DateUtil.getInstant());
                        localDB.getLocalDBEntries().add(localDBEntry);
                    }
                }
            }
        }

        return localDBList;
    }
}
