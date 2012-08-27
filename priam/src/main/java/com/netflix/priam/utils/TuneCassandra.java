package com.netflix.priam.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

@Singleton
public class TuneCassandra extends Task
{
    public static final String JOBNAME = "Tune-Cassandra";
    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);

    @Inject
    public TuneCassandra(IConfiguration config)
    {
        super(config);
    }

    /**
     * update the cassandra yaml file.
     */
    // there is no way we can have uncheck with snake's implementation.
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void updateYaml(IConfiguration config, String yamlLocation, String hostname, String seedProvider) throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        map.put("cluster_name", config.getAppName());
        map.put("storage_port", config.getStoragePort());
        map.put("ssl_storage_port", config.getSSLStoragePort());
        map.put("rpc_port", config.getThriftPort());
        map.put("listen_address", hostname);
        map.put("rpc_address", hostname);
        //Dont bootstrap in restore mode
        map.put("auto_bootstrap", !Restore.isRestoreEnabled(config));
        map.put("saved_caches_directory", config.getCacheLocation());
        map.put("commitlog_directory", config.getCommitLogLocation());
        map.put("data_file_directories", Lists.newArrayList(config.getDataFileLocation()));
        boolean enableIncremental = (config.getBackupHour() >= 0 && config.isIncrBackup()) && (CollectionUtils.isEmpty(config.getBackupRacs()) || config.getBackupRacs().contains(config.getRac()));
        map.put("incremental_backups", enableIncremental);
        map.put("endpoint_snitch", config.getSnitch());
        map.put("in_memory_compaction_limit_in_mb", config.getInMemoryCompactionLimit());
        map.put("compaction_throughput_mb_per_sec", config.getCompactionThroughput());
	    map.put("partitioner", config.getPartitioner());
        
        // messy but needed it for backward and forward compatibilities.
        if (null != map.get("memtable_total_space_in_mb"))
            map.put("memtable_total_space_in_mb", config.getMemtableTotalSpaceMB());
        // the default for stream_throughput_outbound_megabits_per_sec is 7 ms and not in yaml.
        // TODO fixme: currently memtable_total_space_in_mb is used to verify if it is >1.0.7.
        if(null != map.get("memtable_total_space_in_mb"))
            map.put("stream_throughput_outbound_megabits_per_sec", config.getStreamingThroughputMB());
        if(null != map.get("multithreaded_compaction"))
            map.put("multithreaded_compaction", config.getMultithreadedCompaction());
        if (null != map.get("max_hint_window_in_ms"))
        {
            map.put("max_hint_window_in_ms", config.getMaxHintWindowInMS());
            map.put("hinted_handoff_throttle_delay_in_ms", config.getHintHandoffDelay());
        }

        // this is only for 0.8 so check before set.
        if (null != map.get("seed_provider"))
        {
            List<?> seedp = (List) map.get("seed_provider");
            Map<String, String> m = (Map<String, String>) seedp.get(0);
            m.put("class_name", seedProvider);
        }
        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    @SuppressWarnings("unchecked")
    public void updateYaml(boolean autobootstrap) throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(config.getCassHome() + "/conf/cassandra.yaml");
        @SuppressWarnings("rawtypes")
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        //Dont bootstrap in restore mode
        map.put("auto_bootstrap", autobootstrap);
        logger.info("Updating yaml" + yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    @Override
    public void execute() throws IOException
    {
        TuneCassandra.updateYaml(config, config.getCassHome() + "/conf/cassandra.yaml", null, config.getSeedProviderName());
    }

    @Override
    public String getName()
    {
        return "Tune-Cassandra";
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }
}
