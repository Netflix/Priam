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
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/** Created by aagrawal on 11/28/18. */
public class TestMetaFileManager {

    private MetaFileManager metaFileManager;
    private IConfiguration configuration;

    public TestMetaFileManager() {
        Injector injector = Guice.createInjector(new BRTestModule());
        metaFileManager = injector.getInstance(MetaFileManager.class);
        configuration = injector.getInstance(IConfiguration.class);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.cleanDirectory(new File(configuration.getDataFileLocation()));
    }

    @Test
    public void testCleanupOldMetaFiles() throws IOException {
        generateDummyMetaFiles();
        Path dataDir = Paths.get(configuration.getDataFileLocation());
        Assert.assertEquals(4, dataDir.toFile().listFiles().length);

        // clean the directory
        metaFileManager.cleanupOldMetaFiles();

        Assert.assertEquals(1, dataDir.toFile().listFiles().length);
        Path dummy = Paths.get(dataDir.toString(), "dummy.tmp");
        Assert.assertTrue(dummy.toFile().exists());
    }

    private void generateDummyMetaFiles() throws IOException {
        Path dataDir = Paths.get(configuration.getDataFileLocation());
        FileUtils.write(
                Paths.get(
                                configuration.getDataFileLocation(),
                                MetaFileInfo.getMetaFileName(DateUtil.getInstant()))
                        .toFile(),
                "dummy",
                "UTF-8");

        FileUtils.write(
                Paths.get(
                                configuration.getDataFileLocation(),
                                MetaFileInfo.getMetaFileName(
                                        DateUtil.getInstant().minus(10, ChronoUnit.MINUTES)))
                        .toFile(),
                "dummy",
                "UTF-8");

        FileUtils.write(
                Paths.get(
                                configuration.getDataFileLocation(),
                                MetaFileInfo.getMetaFileName(DateUtil.getInstant()) + ".tmp")
                        .toFile(),
                "dummy",
                "UTF-8");

        FileUtils.write(
                Paths.get(configuration.getDataFileLocation(), "dummy.tmp").toFile(),
                "dummy",
                "UTF-8");
    }
}
