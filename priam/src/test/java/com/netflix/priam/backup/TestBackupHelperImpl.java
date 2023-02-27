package com.netflix.priam.backup;

import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.config.BackupsToCompress;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import javax.inject.Provider;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class TestBackupHelperImpl {
    private static final String COMPRESSED_DATA = "compressed-1234-Data.db";
    private static final String COMPRESSION_INFO = "compressed-1234-CompressionInfo.db";
    private static final String UNCOMPRESSED_DATA = "uncompressed-1234-Data.db";
    private static final String RANDOM_DATA = "random-1234-Data.db";
    private static final String RANDOM_COMPONENT = "random-1234-compressioninfo.db";
    private static final ImmutableList<String> TABLE_PARTS =
            ImmutableList.of(
                    COMPRESSED_DATA,
                    COMPRESSION_INFO,
                    UNCOMPRESSED_DATA,
                    RANDOM_DATA,
                    RANDOM_COMPONENT);

    private static final String DIRECTORY = "target/data/ks/cf/backup/";

    @RunWith(Parameterized.class)
    public static class ParameterizedTests {
        private final BackupHelperImpl backupHelper;
        private final String tablePart;
        private final CompressionType compressionAlgorithm;

        @BeforeClass
        public static void setUp() throws IOException {
            FileUtils.forceMkdir(new File(DIRECTORY));
        }

        @Before
        public void createFiles() throws IOException {
            for (String tablePart : TABLE_PARTS) {
                File file = Paths.get(DIRECTORY, tablePart).toFile();
                if (file.createNewFile()) {
                    FileUtils.forceDeleteOnExit(file);
                } else {
                    throw new IllegalStateException("failed to create " + tablePart);
                }
            }
        }

        @AfterClass
        public static void tearDown() throws IOException {
            FileUtils.deleteDirectory(new File(DIRECTORY));
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(
                    new Object[][] {
                        {BackupsToCompress.NONE, COMPRESSED_DATA, CompressionType.NONE},
                        {BackupsToCompress.NONE, COMPRESSION_INFO, CompressionType.NONE},
                        {BackupsToCompress.NONE, UNCOMPRESSED_DATA, CompressionType.NONE},
                        {BackupsToCompress.NONE, RANDOM_DATA, CompressionType.NONE},
                        {BackupsToCompress.NONE, RANDOM_COMPONENT, CompressionType.NONE},
                        {BackupsToCompress.ALL, COMPRESSED_DATA, CompressionType.SNAPPY},
                        {BackupsToCompress.ALL, COMPRESSION_INFO, CompressionType.SNAPPY},
                        {BackupsToCompress.ALL, UNCOMPRESSED_DATA, CompressionType.SNAPPY},
                        {BackupsToCompress.ALL, RANDOM_DATA, CompressionType.SNAPPY},
                        {BackupsToCompress.ALL, RANDOM_COMPONENT, CompressionType.SNAPPY},
                        {BackupsToCompress.IF_REQUIRED, COMPRESSED_DATA, CompressionType.NONE},
                        {BackupsToCompress.IF_REQUIRED, COMPRESSION_INFO, CompressionType.NONE},
                        {BackupsToCompress.IF_REQUIRED, UNCOMPRESSED_DATA, CompressionType.SNAPPY},
                        {BackupsToCompress.IF_REQUIRED, RANDOM_DATA, CompressionType.SNAPPY},
                        {BackupsToCompress.IF_REQUIRED, RANDOM_COMPONENT, CompressionType.SNAPPY},
                    });
        }

        public ParameterizedTests(BackupsToCompress which, String tablePart, CompressionType algo) {
            this.tablePart = tablePart;
            this.compressionAlgorithm = algo;
            Injector injector = Guice.createInjector(new BRTestModule());
            FakeConfiguration fakeConfiguration =
                    (FakeConfiguration) injector.getInstance(IConfiguration.class);
            fakeConfiguration.setFakeConfig("Priam.backupsToCompress", which);
            IFileSystemContext context = injector.getInstance(IFileSystemContext.class);
            Provider<AbstractBackupPath> pathFactory =
                    injector.getProvider(AbstractBackupPath.class);
            backupHelper = new BackupHelperImpl(fakeConfiguration, context, pathFactory);
        }

        @Test
        public void testCorrectCompressionType() throws Exception {
            File parent = new File(DIRECTORY);
            AbstractBackupPath.BackupFileType backupFileType =
                    AbstractBackupPath.BackupFileType.SST_V2;
            ImmutableList<ListenableFuture<AbstractBackupPath>> futures =
                    backupHelper.uploadAndDeleteAllFiles(parent, backupFileType, false);
            AbstractBackupPath abstractBackupPath = null;
            for (ListenableFuture<AbstractBackupPath> future : futures) {
                if (future.get().getFileName().equals(tablePart)) {
                    abstractBackupPath = future.get();
                    break;
                }
            }
            Truth.assertThat(Objects.requireNonNull(abstractBackupPath).getCompression())
                    .isEqualTo(compressionAlgorithm);
        }
    }

    public static class ProgrammaticTests {
        private final BackupHelperImpl backupHelper;
        private final FakeConfiguration config;

        @BeforeClass
        public static void setUp() throws IOException {
            FileUtils.forceMkdir(new File(DIRECTORY));
            for (String tablePart : TABLE_PARTS) {
                File file = Paths.get(DIRECTORY, tablePart).toFile();
                if (file.createNewFile()) {
                    FileUtils.forceDeleteOnExit(file);
                } else {
                    throw new IllegalStateException("failed to create " + tablePart);
                }
            }
        }

        @AfterClass
        public static void tearDown() throws IOException {
            FileUtils.deleteDirectory(new File(DIRECTORY));
        }

        public ProgrammaticTests() {
            Injector injector = Guice.createInjector(new BRTestModule());
            config = (FakeConfiguration) injector.getInstance(IConfiguration.class);
            IFileSystemContext context = injector.getInstance(IFileSystemContext.class);
            Provider<AbstractBackupPath> pathFactory =
                    injector.getProvider(AbstractBackupPath.class);
            backupHelper = new BackupHelperImpl(config, context, pathFactory);
        }

        @Test
        public void testDataFilesAreLast() throws IOException {
            AbstractBackupPath.BackupFileType fileType = AbstractBackupPath.BackupFileType.SST_V2;
            boolean dataFilesAreLast =
                    backupHelper
                            .getBackupPaths(new File(DIRECTORY), fileType)
                            .asList()
                            .stream()
                            .skip(2)
                            .allMatch(p -> p.getBackupFile().getName().endsWith("-Data.db"));
            Truth.assertThat(dataFilesAreLast).isTrue();
        }

        @Test
        public void testNonDataFilesComeFirst() throws IOException {
            AbstractBackupPath.BackupFileType fileType = AbstractBackupPath.BackupFileType.SST_V2;
            boolean nonDataFilesComeFirst =
                    backupHelper
                            .getBackupPaths(new File(DIRECTORY), fileType)
                            .asList()
                            .stream()
                            .limit(2)
                            .noneMatch(p -> p.getBackupFile().getName().endsWith("-Data.db"));
            Truth.assertThat(nonDataFilesComeFirst).isTrue();
        }

        @Test
        public void testNeverCompressedOldFilesAreCompressed() throws IOException {
            AbstractBackupPath.BackupFileType fileType = AbstractBackupPath.BackupFileType.SST_V2;
            long transitionInstant = Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli();
            config.setCompressionTransitionEpochMillis(transitionInstant);
            config.setFakeConfig("Priam.backupsToCompress", BackupsToCompress.NONE);
            boolean backupsAreCompressed =
                    backupHelper
                            .getBackupPaths(new File(DIRECTORY), fileType)
                            .stream()
                            .allMatch(p -> p.getCompression() == CompressionType.SNAPPY);
            Truth.assertThat(backupsAreCompressed).isTrue();
        }

        @Test
        public void testOptionallyCompressedOldFilesAreCompressed() throws IOException {
            AbstractBackupPath.BackupFileType fileType = AbstractBackupPath.BackupFileType.SST_V2;
            long transitionInstant = Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli();
            config.setCompressionTransitionEpochMillis(transitionInstant);
            config.setFakeConfig("Priam.backupsToCompress", BackupsToCompress.IF_REQUIRED);
            boolean backupsAreCompressed =
                    backupHelper
                            .getBackupPaths(new File(DIRECTORY), fileType)
                            .stream()
                            .allMatch(p -> p.getCompression() == CompressionType.SNAPPY);
            Truth.assertThat(backupsAreCompressed).isTrue();
        }
    }
}
