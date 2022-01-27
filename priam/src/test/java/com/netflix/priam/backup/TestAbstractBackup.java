package com.netflix.priam.backup;

import com.google.appengine.repackaged.com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.config.BackupsToCompress;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
    private AbstractBackup abstractBackup;
    private FakeConfiguration fakeConfiguration;
    private String tablePart;
    private CompressionType compressionAlgorithm;

    @BeforeAll
    public static void setUp() throws IOException {
        FileUtils.forceMkdir(new File(DIRECTORY));
    }

    @BeforeEach
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

    @AfterAll
    public static void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(DIRECTORY));
    }

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

    public void initTestAbstractBackup(
            BackupsToCompress which, String tablePart, CompressionType algo) {
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
                    protected void processColumnFamily(File dir) {}

                    @Override
                    public void execute() {}

                    @Override
                    public String getName() {
                        return null;
                    }
                };
    }

    @MethodSource("data")
    @ParameterizedTest
    public void testCorrectCompressionType(
            BackupsToCompress which, String tablePart, CompressionType algo) throws Exception {
        initTestAbstractBackup(which, tablePart, algo);
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
