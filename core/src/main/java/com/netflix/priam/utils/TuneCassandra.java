package com.netflix.priam.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Singleton
public class TuneCassandra extends Task {
    public static final String JOBNAME = "Tune-Cassandra";
    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);

    @Inject
    private CassandraConfiguration cassandraConfiguration;
    @Inject
    private BackupConfiguration backupConfiguration;
    @Inject
    private AmazonConfiguration amazonConfiguration;

    /**
     * update the cassandra yaml file.
     */
    // there is no way we can have uncheck with snake's implementation.
    @SuppressWarnings ({"unchecked", "rawtypes"})
    public static void updateYaml(CassandraConfiguration cassandraConfiguration, BackupConfiguration backupConfiguration, String availabilityZone, String yamlLocation, String hostIp, String seedProvider) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));

        boolean enableIncremental = (backupConfiguration.getHour() >= 0 && backupConfiguration.isIncrementalEnabled()) && (CollectionUtils.isEmpty(backupConfiguration.getAvailabilityZonesToBackup()) || backupConfiguration.getAvailabilityZonesToBackup().contains(availabilityZone));

        map.put("cluster_name", cassandraConfiguration.getClusterName());
        map.put("storage_port", cassandraConfiguration.getStoragePort());
        map.put("ssl_storage_port", cassandraConfiguration.getSslStoragePort());
        map.put("rpc_port", cassandraConfiguration.getThriftPort());
        map.put("listen_address", hostIp);
        map.put("rpc_address", hostIp);
        map.put("auto_bootstrap", !Restore.isRestoreEnabled(backupConfiguration, availabilityZone)); //Dont bootstrap in restore mode
        map.put("saved_caches_directory", cassandraConfiguration.getCacheLocation());
        map.put("commitlog_directory", backupConfiguration.getCommitLogLocation());
        map.put("data_file_directories", Lists.newArrayList(cassandraConfiguration.getDataLocation()));
        map.put("incremental_backups", enableIncremental);
        map.put("endpoint_snitch", cassandraConfiguration.getEndpointSnitch());
        map.put("in_memory_compaction_limit_in_mb", cassandraConfiguration.getInMemoryCompactionLimitMB());
        map.put("compaction_throughput_mb_per_sec", cassandraConfiguration.getCompactionThroughputMBPerSec());
        map.put("partitioner", cassandraConfiguration.getPartitionerClassName());

        // messy but needed it for backward and forward compatibilities.
        if (null != map.get("memtable_total_space_in_mb")) {
            map.put("memtable_total_space_in_mb", cassandraConfiguration.getMemTableTotalSpaceMB());
        }
        // the default for stream_throughput_outbound_megabits_per_sec is 7 ms and not in yaml.
        // TODO fixme: currently memtable_total_space_in_mb is used to verify if it is >1.0.7.
        if (null != map.get("memtable_total_space_in_mb")) {
            map.put("stream_throughput_outbound_megabits_per_sec", backupConfiguration.getStreamingThroughputMbps());
        }
        if (null != map.get("multithreaded_compaction")) {
            map.put("multithreaded_compaction", backupConfiguration.isMultiThreadedCompaction());
        }
        if (null != map.get("max_hint_window_in_ms")) {
            map.put("max_hint_window_in_ms", cassandraConfiguration.getMaxHintWindowMS());
            map.put("hinted_handoff_throttle_delay_in_ms", cassandraConfiguration.getHintedHandoffThrottleDelayMS());
        }

        // this is only for 0.8 so check before set.
        if (null != map.get("seed_provider")) {
            List<?> seedp = (List) map.get("seed_provider");
            Map<String, Object> m = (Map<String, Object>) seedp.get(0);
            m.put("class_name", seedProvider);
            m.put("parameters", ImmutableList.of(ImmutableMap.of("seeds", "127.0.0.1," + hostIp)));
        }

        configureGlobalCaches(cassandraConfiguration, map);

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    /**
     * Setup the cassandra 1.1 global cache values
     */
    private static void configureGlobalCaches(CassandraConfiguration cassandraConfiguration, Map yaml) {
        final String keyCacheSize = cassandraConfiguration.getKeyCacheSizeInMB();
        if(keyCacheSize != null) {
            yaml.put("key_cache_size_in_mb", Integer.valueOf(keyCacheSize));
            final String keyCount = cassandraConfiguration.getKeyCacheKeysToSave();
            if (keyCount != null) {
                yaml.put("key_cache_keys_to_save", Integer.valueOf(keyCount));
            }
        }

        final String rowCacheSize = cassandraConfiguration.getRowCacheSizeInMB();
        if (rowCacheSize != null) {
            yaml.put("row_cache_size_in_mb", Integer.valueOf(rowCacheSize));
            final String rowCount = cassandraConfiguration.getRowCacheKeysToSave();
            if (rowCount != null) {
                yaml.put("row_cache_keys_to_save", Integer.valueOf(rowCount));
            }
        }
    }

    @SuppressWarnings ("unchecked")
    public void updateYaml(boolean autobootstrap) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(cassandraConfiguration.getCassHome() + "/conf/cassandra.yaml");
        @SuppressWarnings ("rawtypes")
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        //Dont bootstrap in restore mode
        map.put("auto_bootstrap", autobootstrap);
        logger.info("Updating yaml" + yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    @Override
    public void execute() throws IOException {
        TuneCassandra.updateYaml(cassandraConfiguration, backupConfiguration, amazonConfiguration.getAvailabilityZone(), cassandraConfiguration.getCassHome() + "/conf/cassandra.yaml", amazonConfiguration.getPrivateIP(), cassandraConfiguration.getSeedProviderClassName());
    }

    @Override
    public String getName() {
        return "Tune-Cassandra";
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }
}
