package com.netflix.priam.resources;

import static org.junit.Assert.assertEquals;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.*;
import com.netflix.priam.backupv2.MetaV2Proxy;
import com.netflix.priam.backupv2.SnapshotMetaTask;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.restore.Restore;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import javax.inject.Provider;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import mockit.Expectations;
import mockit.Mocked;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class BackupServletV2Test {
    private IConfiguration config;
    private @Mocked Restore restoreObj;
    private @Mocked SnapshotMetaTask snapshotBackup;
    private @Mocked BackupVerification backupVerification;
    private @Mocked FileSnapshotStatusMgr backupStatusMgr;
    private @Mocked BackupRestoreUtil backupRestoreUtil;
    private @Mocked MetaV2Proxy metaV2Proxy;
    private BackupServletV2 resource;
    private RestoreServlet restoreResource;
    private InstanceInfo instanceInfo;
    private static final String backupDate = "201812011000";
    private static final Path location =
            Paths.get(
                    "some_bucket/casstestbackup/1049_fake-app/1808575600",
                    AbstractBackupPath.BackupFileType.META_V2.toString(),
                    "1859817645000",
                    "SNAPPY",
                    "PLAINTEXT",
                    "meta_v2_201812011000.json");
    private static Provider<AbstractBackupPath> pathProvider;
    private static IConfiguration configuration;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new BRTestModule());
        config = injector.getInstance(IConfiguration.class);
        instanceInfo = injector.getInstance(InstanceInfo.class);
        resource = injector.getInstance(BackupServletV2.class);
        restoreResource = injector.getInstance(RestoreServlet.class);
        pathProvider = injector.getProvider(AbstractBackupPath.class);
        configuration = injector.getInstance(IConfiguration.class);
    }

    @Test
    public void testBackup() throws Exception {
        new Expectations() {
            {
                snapshotBackup.execute();
            }
        };

        Response response = resource.backup();
        assertEquals(200, response.getStatus());
        assertEquals("[\"ok\"]", response.getEntity());
        assertEquals(
                MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    @Test
    public void testRestoreMinimal() throws Exception {
        final String dateRange = null;
        final String oldRegion = "us-east-1";
        new Expectations() {
            {
                instanceInfo.getRegion();
                result = oldRegion;

                restoreObj.restore(new DateUtil.DateRange((Instant) any, (Instant) any));
            }
        };

        expectCassandraStartup();

        Response response = restoreResource.restore(dateRange);
        assertEquals(200, response.getStatus());
        assertEquals("[\"ok\"]", response.getEntity());
        assertEquals(
                MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    @Test
    public void testRestoreWithDateRange() throws Exception {
        final String dateRange = "201101010000,201112312359";

        new Expectations() {

            {
                DateUtil.getDate(dateRange.split(",")[0]);
                result = new DateTime(2011, 1, 1, 0, 0).toDate();
                times = 1;
                DateUtil.getDate(dateRange.split(",")[1]);
                result = new DateTime(2011, 12, 31, 23, 59).toDate();
                times = 1;
                restoreObj.restore(new DateUtil.DateRange(dateRange));
            }
        };

        expectCassandraStartup();

        Response response = restoreResource.restore(dateRange);
        assertEquals(200, response.getStatus());
        assertEquals("[\"ok\"]", response.getEntity());
        assertEquals(
                MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    // TODO: create CassandraController interface and inject, instead of static util method
    private void expectCassandraStartup() {
        new Expectations() {
            {
                config.getCassStartupScript();
                result = "/usr/bin/false";
                config.getHeapNewSize();
                result = "2G";
                config.getHeapSize();
                result = "8G";
                config.getDataFileLocation();
                result = "/var/lib/cassandra/data";
                config.getCommitLogLocation();
                result = "/var/lib/cassandra/commitlog";
                config.getBackupLocation();
                result = "backup";
                config.getCacheLocation();
                result = "/var/lib/cassandra/saved_caches";
                config.getJmxPort();
                result = 7199;
                config.getMaxDirectMemory();
                result = "50G";
            }
        };
    }

    @Test
    public void testValidate() throws Exception {
        new Expectations() {
            {
                backupVerification.verifyLatestBackup(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        anyBoolean,
                        new DateUtil.DateRange((Instant) any, (Instant) any));
                result = Optional.of(getBackupVerificationResult());
            }
        };
        Response response =
                resource.validateV2SnapshotByDate(
                        new DateUtil.DateRange(Instant.now(), Instant.now()).toString(), true);
        assertEquals(200, response.getStatus());
        assertEquals(
                GsonJsonSerializer.getGson().toJson(getBackupVerificationResult()),
                response.getEntity().toString());
    }

    @Test
    public void testValidateNoBackups() throws Exception {
        new Expectations() {
            {
                backupVerification.verifyLatestBackup(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        anyBoolean,
                        new DateUtil.DateRange((Instant) any, (Instant) any));
                result = Optional.empty();
            }
        };
        Response response =
                resource.validateV2SnapshotByDate(
                        new DateUtil.DateRange(Instant.now(), Instant.now()).toString(), true);
        assertEquals(204, response.getStatus());
        assertEquals(
                response.getEntity().toString(), "No valid meta found for provided time range");
    }

    @Test
    public void testValidateV2SnapshotByDate() throws Exception {
        new Expectations() {
            {
                backupVerification.verifyLatestBackup(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        anyBoolean,
                        new DateUtil.DateRange((Instant) any, (Instant) any));
                result = Optional.of(getBackupVerificationResult());
            }
        };
        Response response =
                resource.validateV2SnapshotByDate(
                        new DateUtil.DateRange(Instant.now(), Instant.now()).toString(), true);
        assertEquals(200, response.getStatus());
        assertEquals(
                GsonJsonSerializer.getGson().toJson(getBackupVerificationResult()),
                response.getEntity().toString());
    }

    //    @Test
    //    public void testListDateRange() throws Exception {
    //        Optional<AbstractBackupPath> abstractBackupPath = getAbstractBackupPath();
    //        String dateRange = String.format("%s,%s",
    //                new SimpleDateFormat("yyyymmddhhmm").format(new Date())
    //                , new SimpleDateFormat("yyyymmddhhmm").format(new Date()));
    //        new Expectations() {{
    //                backupRestoreUtil.getLatestValidMetaPath(metaV2Proxy,
    //                        new DateUtil.DateRange((Instant) any, (Instant) any)); result =
    // abstractBackupPath;
    //
    //                backupRestoreUtil.getAllFiles(
    //                        abstractBackupPath.get(),
    //                        new DateUtil.DateRange((Instant) any, (Instant) any), metaV2Proxy,
    //                        pathProvider); result = getBackupPathList();
    //        }};
    //
    //        Response response =
    //                resource.list(dateRange);
    //        assertEquals(200, response.getStatus());
    //    }

    @Test
    public void testListDateRangeNoBackups() throws Exception {
        String dateRange =
                String.format(
                        "%s,%s",
                        new SimpleDateFormat("yyyymmdd").format(new Date()),
                        new SimpleDateFormat("yyyymmdd").format(new Date()));

        new Expectations() {
            {
                backupRestoreUtil.getLatestValidMetaPath(
                        metaV2Proxy, new DateUtil.DateRange((Instant) any, (Instant) any));
                result = Optional.empty();
            }
        };
        Response response = resource.list(dateRange);
        assertEquals(200, response.getStatus());
        assertEquals(response.getEntity().toString(), "No valid meta found!");
    }

    @Test
    public void testBackUpInfo() throws Exception {
        List<BackupMetadata> backupMetadataList = new ArrayList<>();
        backupMetadataList.add(getBackupMetaData());
        new Expectations() {
            {
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        new DateUtil.DateRange((Instant) any, (Instant) any));
                result = backupMetadataList;
            }
        };
        Response response = resource.info(backupDate);
        assertEquals(200, response.getStatus());
        assertEquals(
                GsonJsonSerializer.getGson().toJson(backupMetadataList),
                response.getEntity().toString());
    }

    @Test
    public void testBackUpInfoNoBackups() {
        new Expectations() {
            {
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        new DateUtil.DateRange((Instant) any, (Instant) any));
                result = new ArrayList<>();
            }
        };
        Response response = resource.info(backupDate);
        assertEquals(200, response.getStatus());
        assertEquals(
                GsonJsonSerializer.getGson().toJson(new ArrayList<>()),
                response.getEntity().toString());
    }

    private static BackupVerificationResult getBackupVerificationResult() {
        BackupVerificationResult result = new BackupVerificationResult();
        result.valid = true;
        result.manifestAvailable = true;
        result.remotePath = "some_random";
        result.filesMatched = 123;
        result.snapshotInstant = Instant.EPOCH;
        return result;
    }

    private static BackupMetadata getBackupMetaData() throws Exception {
        BackupMetadata backupMetadata =
                new BackupMetadata(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        "123",
                        new Date(DateUtil.parseInstant(backupDate).toEpochMilli()));
        backupMetadata.setCompleted(
                new Date(
                        DateUtil.parseInstant(backupDate)
                                .plus(30, ChronoUnit.MINUTES)
                                .toEpochMilli()));
        backupMetadata.setStatus(Status.FINISHED);
        backupMetadata.setSnapshotLocation(location.toString());
        return backupMetadata;
    }

    private static Optional<AbstractBackupPath> getAbstractBackupPath() throws Exception {
        Path path =
                Paths.get(
                        configuration.getDataFileLocation(),
                        "keyspace1",
                        "columnfamily1",
                        "backup",
                        "mc-1234-Data.db");
        AbstractBackupPath abstractBackupPath = pathProvider.get();
        abstractBackupPath.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.SST_V2);
        return Optional.of(abstractBackupPath);
    }

    private static List<AbstractBackupPath> getBackupPathList() throws Exception {
        List<AbstractBackupPath> abstractBackupPathList = new ArrayList<>();
        Path path =
                Paths.get(
                        configuration.getDataFileLocation(),
                        "keyspace1",
                        "columnfamily1",
                        "backup",
                        "mc-1234-Data.db");
        AbstractBackupPath abstractBackupPath1 = pathProvider.get();
        abstractBackupPath1.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.SST_V2);
        abstractBackupPathList.add(abstractBackupPath1);

        path =
                Paths.get(
                        configuration.getDataFileLocation(),
                        "keyspace1",
                        "columnfamily1",
                        "backup",
                        "mc-1234-Data.db");
        AbstractBackupPath abstractBackupPath2 = pathProvider.get();
        abstractBackupPath2.parseLocal(
                path.toFile(), AbstractBackupPath.BackupFileType.SNAPSHOT_VERIFIED);
        abstractBackupPathList.add(abstractBackupPath2);
        return abstractBackupPathList;
    }
}
