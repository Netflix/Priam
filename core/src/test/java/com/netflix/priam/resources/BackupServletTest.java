package com.netflix.priam.resources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.PriamServer;
import com.netflix.priam.TestAmazonConfiguration;
import com.netflix.priam.TestBackupConfiguration;
import com.netflix.priam.TestCassandraConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TuneCassandra;
import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrict;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BackupServletTest {
    private static final Map<String, String> RESULT_OK = ImmutableMap.of("result", "ok");

    private @NonStrict PriamServer priamServer;
    private @NonStrict TestCassandraConfiguration cassandraConfiguration;
    private @NonStrict TestAmazonConfiguration amazonConfiguration;
    private @NonStrict TestBackupConfiguration backupConfiguration;
    private @Mocked IBackupFileSystem fs;
    private @Mocked Restore restoreObj;
    private @Mocked Provider<AbstractBackupPath> pathProvider;
    private @Mocked TuneCassandra tuneCassandra;
    private @Mocked SnapshotBackup snapshotBackup;
    private @Mocked IPriamInstanceRegistry instanceRegistry;
    private @Mocked
    TokenManager tokenManager;
    private BackupServlet resource;

    @Before
    public void setUp() {
        resource = new BackupServlet(priamServer, cassandraConfiguration, amazonConfiguration, backupConfiguration, fs, restoreObj, pathProvider,
            tuneCassandra, snapshotBackup, instanceRegistry, tokenManager);
    }

    @Test
    public void backup() throws Exception {
        new Expectations() {{
            snapshotBackup.execute();
        }};

        Response response = resource.backup();
        assertEquals(200, response.getStatus());
        assertEquals(RESULT_OK, response.getEntity());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    @Test
    public void restore_minimal() throws Exception {
        final String dateRange = null;
        final String newRegion = null;
        final String newToken = null;
        final String keyspaces = null;

        final String oldRegion = "us-east-1";
        final String oldToken = "1234";

        new Expectations() {
            @NonStrict InstanceIdentity identity;
            PriamInstance instance;
  
            {
                amazonConfiguration.getRegionName(); result = oldRegion;
                priamServer.getInstanceIdentity(); result = identity; times = 2;
                identity.getInstance(); result = instance; times = 2;
                instance.getToken(); result = oldToken;

                backupConfiguration.isRestoreClosestToken(); result = false;
  
                restoreObj.restore((Date) any, (Date) any); // TODO: test default value

                amazonConfiguration.setRegionName(oldRegion);
                instance.setToken(oldToken);
                tuneCassandra.updateYaml(false);
            }
        };

        expectCassandraStartup();

        Response response = resource.restore(dateRange, newRegion, newToken, keyspaces, null);
        assertEquals(200, response.getStatus());
        assertEquals(RESULT_OK, response.getEntity());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    @Test
    public void restore_withDateRange() throws Exception {
        final String dateRange = "201101010000,201112312359";
        final String newRegion = null;
        final String newToken = null;
        final String keyspaces = null;

        final String oldRegion = "us-east-1";
        final String oldToken = "1234";

        new Expectations() {
            @NonStrict InstanceIdentity identity;
            PriamInstance instance;
            AbstractBackupPath backupPath;
  
            {
                pathProvider.get(); result = backupPath;
                backupPath.getFormat(); result = AbstractBackupPath.DAY_FORMAT; times = 2;

                amazonConfiguration.getRegionName(); result = oldRegion;
                priamServer.getInstanceIdentity(); result = identity; times = 2;
                identity.getInstance(); result = instance; times = 2;
                instance.getToken(); result = oldToken;

                backupConfiguration.isRestoreClosestToken(); result = false;

                restoreObj.restore(
                    new DateTime(2011, 01, 01, 00, 00).toDate(),
                    new DateTime(2011, 12, 31, 23, 59).toDate());

                amazonConfiguration.setRegionName(oldRegion);
                instance.setToken(oldToken);
                tuneCassandra.updateYaml(false);
            }
        };

        expectCassandraStartup();

        Response response = resource.restore(dateRange, newRegion, newToken, keyspaces, null);
        assertEquals(200, response.getStatus());
        assertEquals(RESULT_OK, response.getEntity());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

//    @Test
//    public void restore_withRegion() throws Exception
//    {
//        final String dateRange = null;
//        final String newRegion = "us-west-1";
//        final String newToken = null;
//        final String keyspaces = null;
//
//        final String oldRegion = "us-east-1";
//        final String oldToken = "1234";
//        final String appName = "myApp";
//
//        new Expectations() {
//            @NonStrict InstanceIdentity identity;
//            PriamInstance instance;
//            @NonStrict PriamInstance instance1, instance2, instance3;
//  
//            {
//                cassandraConfiguration.getRegionName(); result = oldRegion;
//                priamServer.getInstanceIdentity(); result = identity; times = 3;
//                identity.getInstance(); result = instance; times = 3;
//                instance.getToken(); result = oldToken;
//
//                cassandraConfiguration.isRestoreClosestToken(); result = false;
//                
//                cassandraConfiguration.setRegionName(newRegion);
//                instance.getToken(); result = oldToken;
//                cassandraConfiguration.getAppName(); result = appName;
//                instanceRegistry.getAllIds(appName); result = ImmutableList.of(instance, instance1, instance2, instance3);
//                instance.getRegionName();  result = oldRegion;
//                instance.getToken(); result = oldToken;
//                instance1.getRegionName(); result = oldRegion;
//                instance2.getRegionName(); result = oldRegion;
//                instance3.getRegionName(); result = oldRegion;
//                instance1.getToken(); result = "1234";
//                instance2.getToken(); result = "5678";
//                instance3.getToken(); result = "9000";
//                instance.setToken((String) any); // TODO: test mocked closest token
//
//                restoreObj.restore((Date) any, (Date) any); // TODO: test default value
//  
//                cassandraConfiguration.setRegionName(oldRegion);
//                instance.setToken(oldToken);
//                tuneCassandra.updateYaml(false);
//            }
//        };
//
//        expectCassandraStartup();
//
//        Response response = resource.restore(dateRange, newRegion, newToken, keyspaces);
//        assertEquals(200, response.getStatus());
//        assertEquals(RESULT_OK, response.getEntity());
//        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
//    }

    @Test
    public void restore_withToken() throws Exception {
        final String dateRange = null;
        final String newRegion = null;
        final String newToken = "myNewToken";
        final String keyspaces = null;

        final String oldRegion = "us-east-1";
        final String oldToken = "1234";

        new Expectations() {
            @NonStrict InstanceIdentity identity;
            PriamInstance instance;
  
            {
                amazonConfiguration.getRegionName(); result = oldRegion;
                priamServer.getInstanceIdentity(); result = identity; times = 3;
                identity.getInstance(); result = instance; times = 3;
                instance.getToken(); result = oldToken;
                instance.setToken(newToken);

                backupConfiguration.isRestoreClosestToken(); result = false;

                restoreObj.restore((Date) any, (Date) any); // TODO: test default value

                amazonConfiguration.setRegionName(oldRegion);
                instance.setToken(oldToken);
                tuneCassandra.updateYaml(false);
            }
        };

        expectCassandraStartup();

        Response response = resource.restore(dateRange, newRegion, newToken, keyspaces, null);
        assertEquals(200, response.getStatus());
        assertEquals(RESULT_OK, response.getEntity());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    @Test
    public void restore_withKeyspaces() throws Exception {
        final String dateRange = null;
        final String newRegion = null;
        final String newToken = null;
        final String keyspaces = "keyspace1,keyspace2";

        final String oldRegion = "us-east-1";
        final String oldToken = "1234";

        new Expectations() {
            @NonStrict InstanceIdentity identity;
            PriamInstance instance;
  
            {
                amazonConfiguration.getRegionName(); result = oldRegion;
                priamServer.getInstanceIdentity(); result = identity; times = 2;
                identity.getInstance(); result = instance; times = 2;
                instance.getToken(); result = oldToken;

                backupConfiguration.isRestoreClosestToken(); result = false;
  
                List<String> restoreKeyspaces = Lists.newArrayList();
                backupConfiguration.getRestoreKeyspaces(); result = restoreKeyspaces;
                restoreKeyspaces.clear();
                restoreKeyspaces.addAll(ImmutableList.of("keyspace1", "keyspace2"));

                restoreObj.restore((Date) any, (Date) any); // TODO: test default value

                amazonConfiguration.setRegionName(oldRegion);
                instance.setToken(oldToken);
                tuneCassandra.updateYaml(false);
            }
        };

        expectCassandraStartup();

        Response response = resource.restore(dateRange, newRegion, newToken, keyspaces, null);
        assertEquals(200, response.getStatus());
        assertEquals(RESULT_OK, response.getEntity());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    // TODO: this should also set/test newRegion and keyspaces
    @Test
    public void restore_maximal() throws Exception {
        final String dateRange = "201101010000,201112312359";
        final String newRegion = null;
        final String newToken = "5678";
        final String keyspaces = null;

        final String oldRegion = "us-east-1";
        final String oldToken = "1234";
        final String appName = "myApp";

        new Expectations() {
            @NonStrict InstanceIdentity identity;
            PriamInstance instance;
            @NonStrict PriamInstance instance1, instance2, instance3;
            AbstractBackupPath backupPath;

            {
                pathProvider.get(); result = backupPath;
                backupPath.getFormat(); result = AbstractBackupPath.DAY_FORMAT; times = 2;

                amazonConfiguration.getRegionName(); result = oldRegion; times = 1;
                priamServer.getInstanceIdentity(); result = identity; times = 5;
                identity.getInstance(); result = instance; times = 5;
                instance.getToken(); result = oldToken;
                instance.setToken(newToken);

                backupConfiguration.isRestoreClosestToken(); result = true;
                instance.getToken(); result = oldToken;
                cassandraConfiguration.getClusterName(); result = appName;
                instanceRegistry.getAllIds(appName); result = ImmutableList.of(instance, instance1, instance2, instance3);
                instance.getRegionName();  result = oldRegion;
                instance.getToken(); result = oldToken;
                instance1.getRegionName(); result = oldRegion;
                instance2.getRegionName(); result = oldRegion;
                instance3.getRegionName(); result = oldRegion;
                instance1.getToken(); result = "1234";
                instance2.getToken(); result = "5678";
                instance3.getToken(); result = "9000";
                instance.setToken((String) any); // TODO: test mocked closest token

                restoreObj.restore(
                    new DateTime(2011, 01, 01, 00, 00).toDate(),
                    new DateTime(2011, 12, 31, 23, 59).toDate());
  
                amazonConfiguration.setRegionName(oldRegion);
                instance.setToken(oldToken);
                tuneCassandra.updateYaml(false);
            }
        };

        expectCassandraStartup();

        Response response = resource.restore(dateRange, newRegion, newToken, keyspaces, null);
        assertEquals(200, response.getStatus());
        assertEquals(RESULT_OK, response.getEntity());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    // TODO: create CassandraController interface and inject, instead of static util method
    private Expectations expectCassandraStartup() {
        return new Expectations() {{
            amazonConfiguration.getInstanceType(); result = "m1.xlarge";
            cassandraConfiguration.getCassStartScript(); result = "/usr/bin/false";
            cassandraConfiguration.getMaxNewGenHeapSize(); result = ImmutableMap.of("m1.xlarge", "2G");
            cassandraConfiguration.getMaxHeapSize(); result = ImmutableMap.of("m1.xlarge", "8G");
            cassandraConfiguration.getDataLocation(); result = "/var/lib/cassandra/data";
            backupConfiguration.getCommitLogLocation(); result = "/var/lib/cassandra/commitlog";
            backupConfiguration.getS3BaseDir(); result = "backup";
            cassandraConfiguration.getCacheLocation(); result = "/var/lib/cassandra/saved_caches";
            cassandraConfiguration.getHeapDumpLocation(); result = "/var/log/cassandra/heaps";
            cassandraConfiguration.getJmxPort(); result = 7199;
            cassandraConfiguration.getDirectMaxHeapSize(); result = ImmutableMap.of("m1.xlarge", "50G");
        }};
    }
}