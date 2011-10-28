package com.priam.netflix;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;
import com.netflix.cassandra.KeyspaceFactory;
import com.netflix.cassandra.NFAstyanaxManager;
import com.netflix.instance.identity.StorageDevice;
import com.netflix.library.NFLibraryManager;
import com.priam.conf.IConfiguration;
import com.priam.identity.PriamInstance;

/**
 * Use bootstrap cluster to find tokens and nodes in the ring
 * 
 * @author Praveen Sadhu
 * 
 */
@Singleton
public class InstanceDataDAOCassandra
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceDataDAOCassandra.class);
    private static final String CN_ID = "Id";
    private static final String CN_APPID = "appId";
    private static final String CN_AZ = "availabilityZone";
    private static final String CN_INSTANCEID = "instanceId";
    private static final String CN_HOSTNAME = "hostname";
    private static final String CN_EIP = "elasticIP";
    private static final String CN_TOKEN = "token";
    private static final String CN_LOCATION = "location";
    private static final String CN_VOLUME_PREFIX = "ssVolumes";
    private static final String CN_UPDATETIME = "updatetime";

    private Keyspace bootKeyspace;

    /*
     * Schema: create column family tokens with comparator=UTF8Type and
     * column_metadata=[ {column_name: appId, validation_class:
     * UTF8Type,index_type: KEYS}, {column_name: instanceId, validation_class:
     * UTF8Type}, {column_name: token, validation_class: UTF8Type},
     * {column_name: availabilityZone, validation_class: UTF8Type},
     * {column_name: hostname, validation_class: UTF8Type},{column_name: Id,
     * validation_class: UTF8Type}, {column_name: elasticIP, validation_class:
     * UTF8Type}, {column_name: updatetime, validation_class: TimeUUIDType},
     * {column_name: location, validation_class: UTF8Type}];
     */
    public static final ColumnFamily<String, String> CF_TOKENS = new ColumnFamily<String, String>("tokens", StringSerializer.get(), StringSerializer.get());
    // Schema: create column family locks with comparator=UTF8Type;
    public static final ColumnFamily<String, String> CF_LOCKS = new ColumnFamily<String, String>("locks", StringSerializer.get(), StringSerializer.get());

    @Inject
    public InstanceDataDAOCassandra(IConfiguration config) {

        Properties props = new Properties();
        props.setProperty(config.getBootClusterName() + ".bootstrap.astyanax.readConsistency", "CL_QUORUM");
        props.setProperty(config.getBootClusterName() + ".bootstrap.astyanax.writeConsistency", "CL_QUORUM");

        try {
            NFLibraryManager.initLibrary(NFAstyanaxManager.class, props, true, false);
            bootKeyspace = KeyspaceFactory.openKeyspace(config.getBootClusterName(), "cassbootstrap");
        }
        catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

    }

    public void createInstanceEntry(PriamInstance instance) throws Exception
    {
        String key = getRowKey(instance);
        // If the key exists throw exception
        if (getInstance(instance.getApp(), instance.getId()) != null)
            throw new Exception(String.format("Key already exists: %s", key));
        // Grab the lock
        getLock(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> clm = m.withRow(CF_TOKENS, key);
        clm.putColumn(CN_ID, Integer.toString(instance.getId()), null);
        clm.putColumn(CN_APPID, instance.getApp(), null);
        clm.putColumn(CN_AZ, instance.getRac(), null);
        clm.putColumn(CN_INSTANCEID, instance.getInstanceId(), null);
        clm.putColumn(CN_HOSTNAME, instance.getHostName(), null);
        clm.putColumn(CN_EIP, instance.getHostIP(), null);
        clm.putColumn(CN_TOKEN, instance.getPayload(), null);
        clm.putColumn(CN_LOCATION, instance.getDC(), null);
        clm.putColumn(CN_UPDATETIME, TimeUUIDUtils.getUniqueTimeUUIDinMicros(), null);
        Map<String, StorageDevice> volumes = instance.getVolumes();
        if (volumes != null) {
            for (String path : volumes.keySet()) {
                clm.putColumn(CN_VOLUME_PREFIX + "_" + path, volumes.get(path).toString(), null);
            }
        }
        m.execute();
    }

    /*
     * To get a lock on the row - Create a choosing row and make sure there are
     * no contenders. If there are bail out. Also delete the column when bailing
     * out. - Once there are no contenders, grab the lock if it is not already
     * taken.
     */
    private void getLock(PriamInstance instance) throws Exception
    {

        String choosingkey = getChoosingKey(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> clm = m.withRow(CF_LOCKS, choosingkey);

        // Expire in 6 sec
        clm.putColumn(instance.getInstanceId(), instance.getInstanceId(), new Integer(6));
        m.execute();
        int count = bootKeyspace.prepareQuery(CF_LOCKS).getKey(choosingkey).getCount().execute().getResult();
        if (count > 1) {
            // Need to delete my entry
            m.withRow(CF_LOCKS, choosingkey).deleteColumn(instance.getInstanceId());
            m.execute();
            throw new Exception(String.format("More than 1 contender for lock %s %d", choosingkey, count));
        }

        String lockKey = getLockingKey(instance);
        OperationResult<ColumnList<String>> result = bootKeyspace.prepareQuery(CF_LOCKS).getKey(lockKey).execute();
        if (result.getResult().size() > 0 && !result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId()))
            throw new Exception(String.format("Lock already taken %s", lockKey));

        clm = m.withRow(CF_LOCKS, lockKey);
        clm.putColumn(instance.getInstanceId(), instance.getInstanceId(), new Integer(600));
        m.execute();
        Thread.sleep(100);
        result = bootKeyspace.prepareQuery(CF_LOCKS).getKey(lockKey).execute();
        if (result.getResult().size() == 1 && result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId())) {
            logger.info("Got lock " + lockKey);
            return;
        }
        else
            throw new Exception(String.format("Cannot insert lock %s", lockKey));

    }

    public void deleteInstanceEntry(PriamInstance instance) throws Exception
    {
        // Acquire the lock first
        getLock(instance);

        // Delete the row
        String key = getRowKey(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_TOKENS, key).delete();
        m.execute();

        key = getLockingKey(instance);
        // Delete key
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();

        // Have to delete choosing key as well to avoid issues with delete
        // followed by immediate writes
        key = getChoosingKey(instance);
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();

    }

    public PriamInstance getInstance(String app, int id)
    {
        Set<PriamInstance> set = getAllInstances(app);
        for (PriamInstance ins : set) {
            if (ins.getId() == id)
                return ins;
        }
        return null;
    }

    public Set<PriamInstance> getAllInstances(String app)
    {
        Set<PriamInstance> set = new HashSet<PriamInstance>();
        try {
            bootKeyspace.prepareQuery(CF_TOKENS).searchWithIndex();
            OperationResult<Rows<String, String>> result;
            result = bootKeyspace.prepareQuery(CF_TOKENS).searchWithIndex().setStartKey("").addExpression().whereColumn(CN_APPID).equals().value(app).execute();

            for (Row<String, String> row : result.getResult())
                set.add(transform(row.getColumns()));
        }
        catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
        return set;
    }

    private PriamInstance transform(ColumnList<String> columns)
    {
        PriamInstance ins = new PriamInstance();
        Map<String, StorageDevice> volumes = null;
        Map<String, String> cmap = new HashMap<String, String>();
        for (Column<String> column : columns) {
            cmap.put(column.getName(), column.getStringValue());
            if (column.getName().startsWith(CN_VOLUME_PREFIX)) {
                String[] volName = column.getName().split("_");
                StorageDevice sd = new StorageDevice();
                sd.deserialize(column.getStringValue());
                if (volumes == null)
                    volumes = new HashMap<String, StorageDevice>();
                volumes.put(volName[1], sd);
            }
            if (column.getName().equals(CN_APPID))
                ins.setUpdatetime(column.getTimestamp());
        }

        ins.setApp(cmap.get(CN_APPID));
        ins.setRac(cmap.get(CN_AZ));
        ins.setHost(cmap.get(CN_HOSTNAME));
        ins.setHostIP(cmap.get(CN_EIP));
        ins.setId(Integer.parseInt(cmap.get(CN_ID)));
        ins.setInstanceId(cmap.get(CN_INSTANCEID));
        ins.setDC(cmap.get(CN_LOCATION));
        ins.setPayload(cmap.get(CN_TOKEN));
        ins.setVolumes(volumes);
        return ins;
    }

    private String getChoosingKey(PriamInstance instance)
    {
        return instance.getApp() + instance.getId() + "-choosing";
    }

    private String getLockingKey(PriamInstance instance)
    {
        return instance.getApp() + instance.getId() + "-lock";
    }

    private String getRowKey(PriamInstance instance)
    {
        return instance.getApp() + instance.getId();
    }

}
