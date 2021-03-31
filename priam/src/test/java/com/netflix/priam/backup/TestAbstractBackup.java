package com.netflix.priam.backup;

import com.google.appengine.repackaged.com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.netflix.priam.compress.CompressionAlgorithm;
import com.netflix.priam.config.BackupsToCompress;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestAbstractBackup {
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
    private final AbstractBackup abstractBackup;
    private final FakeConfiguration fakeConfiguration;
    private final String tablePart;
    private final CompressionAlgorithm compressionAlgorithm;

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
                    {BackupsToCompress.NONE, COMPRESSED_DATA, CompressionAlgorithm.NONE},
                    {BackupsToCompress.NONE, COMPRESSION_INFO, CompressionAlgorithm.NONE},
                    {BackupsToCompress.NONE, UNCOMPRESSED_DATA, CompressionAlgorithm.NONE},
                    {BackupsToCompress.NONE, RANDOM_DATA, CompressionAlgorithm.NONE},
                    {BackupsToCompress.NONE, RANDOM_COMPONENT, CompressionAlgorithm.NONE},
                    {BackupsToCompress.ALL, COMPRESSED_DATA, CompressionAlgorithm.SNAPPY},
                    {BackupsToCompress.ALL, COMPRESSION_INFO, CompressionAlgorithm.SNAPPY},
                    {BackupsToCompress.ALL, UNCOMPRESSED_DATA, CompressionAlgorithm.SNAPPY},
                    {BackupsToCompress.ALL, RANDOM_DATA, CompressionAlgorithm.SNAPPY},
                    {BackupsToCompress.ALL, RANDOM_COMPONENT, CompressionAlgorithm.SNAPPY},
                    {BackupsToCompress.IF_REQUIRED, COMPRESSED_DATA, CompressionAlgorithm.NONE},
                    {BackupsToCompress.IF_REQUIRED, COMPRESSION_INFO, CompressionAlgorithm.NONE},
                    {BackupsToCompress.IF_REQUIRED, UNCOMPRESSED_DATA, CompressionAlgorithm.SNAPPY},
                    {BackupsToCompress.IF_REQUIRED, RANDOM_DATA, CompressionAlgorithm.SNAPPY},
                    {BackupsToCompress.IF_REQUIRED, RANDOM_COMPONENT, CompressionAlgorithm.SNAPPY},
                });
    }

    public TestAbstractBackup(
            BackupsToCompress which, String tablePart, CompressionAlgorithm algo) {
        this.tablePart = tablePart;
        this.compressionAlgorithm = algo;
        Injector injector = Guice.createInjector(new BRTestModule());
        fakeConfiguration = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        fakeConfiguration.setFakeConfig("Priam.backupsToCompress", which);
        IFileSystemContext context = injector.getInstance(IFileSystemContext.class);
        Provider<AbstractBackupPath> pathFactory = injector.getProvider(AbstractBackupPath.class);
        abstractBackup =
                new AbstractBackup(fakeConfiguration, context, pathFactory) {
                    @Override
                    protected void processColumnFamily(String ks, String cf, File dir) {}

                    @Override
                    public void execute() {}

                    @Override
                    public String getName() {
                        return null;
                    }
                };
    }

    @Test
    public void testCorrectCompressionAlgorithm() throws Exception {
        File parent = new File(DIRECTORY);
        AbstractBackupPath.BackupFileType backupFileType = AbstractBackupPath.BackupFileType.SST_V2;
        ImmutableSet<AbstractBackupPath> paths =
                abstractBackup.upload(parent, backupFileType, false, false);
        AbstractBackupPath abstractBackupPath =
                paths.stream()
                        .filter(path -> path.getFileName().equals(tablePart))
                        .findAny()
                        .orElseThrow(IllegalStateException::new);
        Truth.assertThat(abstractBackupPath.getCompression()).isEqualTo(compressionAlgorithm);
    }
}
