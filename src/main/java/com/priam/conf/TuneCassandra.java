package com.priam.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.HintedHandOffManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.priam.scheduler.SimpleTimer;
import com.priam.scheduler.Task;
import com.priam.scheduler.TaskTimer;

public class TuneCassandra extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);
    public static final String JOBNAME = "Tune-Cassandra";
    private IConfiguration config;

    @Inject
    public TuneCassandra(IConfiguration config)
    {
        this.config = config;
    }

    /**
     * update the cassandra yaml file.
     */
    // there is no way we can have uncheck with snake's implementation.
    @SuppressWarnings("unchecked")
    public void updateYaml() throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(config.getYamlLocation());
        @SuppressWarnings("rawtypes")
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        map.put("cluster_name", config.getAppName());
        map.put("storage_port", 7101);
        map.put("rpc_port", 7102);
        map.put("listen_address", null);
        map.put("rpc_address", null);
        //Dont bootstrap in restore mode
        map.put("auto_bootstrap", config.getRestoreSnapshot().equals("")?true:false);
        map.put("saved_caches_directory", config.getCacheLocation());
        map.put("commitlog_directory", config.getCommitLogLocation());
        map.put("data_file_directories", Lists.newArrayList(config.getDataFileLocation()));
        map.put("incremental_backups", (config.getBackupHour() >= 0 && config.isIncrBackup()) ? true : false);
        map.put("endpoint_snitch", config.getSnitch());
        map.put("in_memory_compaction_limit_in_mb", config.getInMemoryCompactionLimit());
        map.put("compaction_throughput_mb_per_sec", config.getCompactionThroughput());

        // this is only for 0.8 so check before set.
        if (null != map.get("seed_provider"))
        {
            List<?> seedp = (List) map.get("seed_provider");
            Map<String, String> m = (Map<String, String>) seedp.get(0);
            m.put("class_name", "org.apache.cassandra.thrift.NFSeedProvider");
        }
        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    @Override
    public void execute() throws IOException
    {
        updateYaml();
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
