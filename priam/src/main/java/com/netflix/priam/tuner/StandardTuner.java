/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.tuner;

import com.google.common.collect.Lists;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.restore.Restore;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Tune the standard cassandra parameters/configurations. eg. cassandra.yaml, jvm.options, bootstrap
 * etc.
 */
public class StandardTuner implements ICassandraTuner {
    private static final Logger logger = LoggerFactory.getLogger(StandardTuner.class);
    protected final IConfiguration config;
    protected final IBackupRestoreConfig backupRestoreConfig;
    private final InstanceInfo instanceInfo;

    @Inject
    public StandardTuner(
            IConfiguration config,
            IBackupRestoreConfig backupRestoreConfig,
            InstanceInfo instanceInfo) {
        this.config = config;
        this.backupRestoreConfig = backupRestoreConfig;
        this.instanceInfo = instanceInfo;
    }

    @SuppressWarnings("unchecked")
    public void writeAllProperties(String yamlLocation, String hostname, String seedProvider)
            throws Exception {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map map = yaml.load(new FileInputStream(yamlFile));
        map.put("cluster_name", config.getAppName());
        map.put("storage_port", config.getStoragePort());
        map.put("ssl_storage_port", config.getSSLStoragePort());
        map.put("start_rpc", config.isThriftEnabled());
        map.put("rpc_port", config.getThriftPort());
        map.put("start_native_transport", config.isNativeTransportEnabled());
        map.put("native_transport_port", config.getNativeTransportPort());
        map.put("listen_address", hostname);
        map.put("rpc_address", hostname);
        // Dont bootstrap in restore mode
        if (!Restore.isRestoreEnabled(config, instanceInfo)) {
            map.put("auto_bootstrap", config.getAutoBoostrap());
        } else {
            map.put("auto_bootstrap", false);
        }

        map.put("saved_caches_directory", config.getCacheLocation());
        map.put("commitlog_directory", config.getCommitLogLocation());
        map.put("data_file_directories", Lists.newArrayList(config.getDataFileLocation()));

        boolean enableIncremental = IncrementalBackup.isEnabled(config, backupRestoreConfig);
        map.put("incremental_backups", enableIncremental);

        map.put("endpoint_snitch", config.getSnitch());
        if (map.containsKey("in_memory_compaction_limit_in_mb")) {
            map.remove("in_memory_compaction_limit_in_mb");
        }
        map.put("compaction_throughput_mb_per_sec", config.getCompactionThroughput());
        map.put(
                "partitioner",
                derivePartitioner(map.get("partitioner").toString(), config.getPartitioner()));

        if (map.containsKey("memtable_total_space_in_mb")) {
            map.remove("memtable_total_space_in_mb");
        }

        map.put("stream_throughput_outbound_megabits_per_sec", config.getStreamingThroughputMB());
        if (map.containsKey("multithreaded_compaction")) {
            map.remove("multithreaded_compaction");
        }

        map.put("max_hint_window_in_ms", config.getMaxHintWindowInMS());
        map.put("hinted_handoff_throttle_in_kb", config.getHintedHandoffThrottleKb());
        map.put("authenticator", config.getAuthenticator());
        map.put("authorizer", config.getAuthorizer());
        map.put("internode_compression", config.getInternodeCompression());
        map.put("dynamic_snitch", config.isDynamicSnitchEnabled());

        map.put("concurrent_reads", config.getConcurrentReadsCnt());
        map.put("concurrent_writes", config.getConcurrentWritesCnt());
        map.put("concurrent_compactors", config.getConcurrentCompactorsCnt());

        map.put("rpc_server_type", config.getRpcServerType());
        map.put("rpc_min_threads", config.getRpcMinThreads());
        map.put("rpc_max_threads", config.getRpcMaxThreads());
        // Add private ip address as broadcast_rpc_address. This will ensure that COPY function
        // works correctly.
        map.put("broadcast_rpc_address", instanceInfo.getPrivateIP());

        map.put("tombstone_warn_threshold", config.getTombstoneWarnThreshold());
        map.put("tombstone_failure_threshold", config.getTombstoneFailureThreshold());
        map.put("streaming_socket_timeout_in_ms", config.getStreamingSocketTimeoutInMS());

        map.put("memtable_cleanup_threshold", config.getMemtableCleanupThreshold());
        map.put(
                "compaction_large_partition_warning_threshold_mb",
                config.getCompactionLargePartitionWarnThresholdInMB());
        map.put("auto_snapshot", config.getAutoSnapshot());

        List<?> seedp = (List) map.get("seed_provider");
        Map<String, String> m = (Map<String, String>) seedp.get(0);
        m.put("class_name", seedProvider);

        configfureSecurity(map);
        configureGlobalCaches(config, map);
        // force to 1 until vnodes are properly supported
        map.put("num_tokens", 1);

        // Additional C* Yaml properties, which can be set via Priam.extra.params
        addExtraCassParams(map);

        // Custom specific C* yaml properties which might not be available in Apache C* OSS
        addCustomCassParams(map);

        // remove troublesome properties
        map.remove("flush_largest_memtables_at");
        map.remove("reduce_cache_capacity_to");

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));

        // TODO: port commit log backups to the PropertiesFileTuner implementation
        configureCommitLogBackups();

        PropertiesFileTuner propertyTuner = new PropertiesFileTuner(config);
        for (String propertyFile : config.getTunablePropertyFiles()) {
            propertyTuner.updateAndSaveProperties(propertyFile);
        }
    }

    /**
     * This method can be overwritten in child classes for any additional tunings to C* Yaml.
     * Default implementation is left empty intentionally for child classes to override. This is
     * useful when custom YAML properties are supported in deployed C*.
     *
     * @param map
     */
    protected void addCustomCassParams(Map map) {}

    /**
     * Overridable by derived classes to inject a wrapper snitch.
     *
     * @return Sntich to be used by this cluster
     */
    protected String getSnitch() {
        return config.getSnitch();
    }

    /** Setup the cassandra 1.1 global cache values */
    private void configureGlobalCaches(IConfiguration config, Map yaml) {
        final String keyCacheSize = config.getKeyCacheSizeInMB();
        if (!StringUtils.isEmpty(keyCacheSize)) {
            yaml.put("key_cache_size_in_mb", Integer.valueOf(keyCacheSize));

            final String keyCount = config.getKeyCacheKeysToSave();
            if (!StringUtils.isEmpty(keyCount))
                yaml.put("key_cache_keys_to_save", Integer.valueOf(keyCount));
        }

        final String rowCacheSize = config.getRowCacheSizeInMB();
        if (!StringUtils.isEmpty(rowCacheSize)) {
            yaml.put("row_cache_size_in_mb", Integer.valueOf(rowCacheSize));

            final String rowCount = config.getRowCacheKeysToSave();
            if (!StringUtils.isEmpty(rowCount))
                yaml.put("row_cache_keys_to_save", Integer.valueOf(rowCount));
        }
    }

    String derivePartitioner(String fromYaml, String fromConfig) {
        if (fromYaml == null || fromYaml.isEmpty()) return fromConfig;
        // this check is to prevent against overwriting an existing yaml file that has
        // a partitioner not RandomPartitioner or (as of cass 1.2) Murmur3Partitioner.
        // basically we don't want to hose existing deployments by changing the partitioner
        // unexpectedly on them
        final String lowerCase = fromYaml.toLowerCase();
        if (lowerCase.contains("randomparti") || lowerCase.contains("murmur")) return fromConfig;
        return fromYaml;
    }

    protected void configfureSecurity(Map map) {
        // the client-side ssl settings
        Map clientEnc = (Map) map.get("client_encryption_options");
        clientEnc.put("enabled", config.isClientSslEnabled());

        // the server-side (internode) ssl settings
        Map serverEnc = (Map) map.get("server_encryption_options");
        serverEnc.put("internode_encryption", config.getInternodeEncryption());
    }

    protected void configureCommitLogBackups() {
        if (!config.isBackingUpCommitLogs()) return;
        Properties props = new Properties();
        props.put("archive_command", config.getCommitLogBackupArchiveCmd());
        props.put("restore_command", config.getCommitLogBackupRestoreCmd());
        props.put("restore_directories", config.getCommitLogBackupRestoreFromDirs());
        props.put("restore_point_in_time", config.getCommitLogBackupRestorePointInTime());

        try (FileOutputStream fos =
                new FileOutputStream(new File(config.getCommitLogBackupPropsFile()))) {
            props.store(fos, "cassandra commit log archive props, as written by priam");
        } catch (IOException e) {
            logger.error("Could not store commitlog_archiving.properties", e);
        }
    }

    public void updateAutoBootstrap(String yamlFile, boolean autobootstrap) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        @SuppressWarnings("rawtypes")
        Map map = yaml.load(new FileInputStream(yamlFile));
        // Dont bootstrap in restore mode
        map.put("auto_bootstrap", autobootstrap);
        if (logger.isInfoEnabled()) {
            logger.info("Updating yaml: " + yaml.dump(map));
        }
        yaml.dump(map, new FileWriter(yamlFile));
    }

    @Override
    public final void updateJVMOptions() throws Exception {
        if (config.supportsTuningJVMOptionsFile()) {
            JVMOptionsTuner tuner = new JVMOptionsTuner(config);
            // Overwrite default jvm.options file.
            tuner.updateAndSaveJVMOptions(config.getJVMOptionsFileLocation());
        }
    }

    public void addExtraCassParams(Map map) {
        String params = config.getExtraConfigParams();
        if (StringUtils.isEmpty(params)) {
            logger.info("Updating yaml: no extra cass params");
            return;
        }

        String[] pairs = params.split(",");
        logger.info("Updating yaml: adding extra cass params");
        for (String pair1 : pairs) {
            String[] pair = pair1.split("=");
            String priamKey = pair[0];
            String cassKey = pair[1];
            String cassVal = config.getCassYamlVal(priamKey);

            if (!StringUtils.isBlank(cassKey) && !StringUtils.isBlank(cassVal)) {
                if (!cassKey.contains(".")) {
                    logger.info(
                            "Updating yaml: PriamKey: [{}], Key: [{}], OldValue: [{}], NewValue: [{}]",
                            priamKey,
                            cassKey,
                            map.get(cassKey),
                            cassVal);
                    map.put(cassKey, cassVal);
                } else {
                    // split the cassandra key. We will get the group and get the key name.
                    String[] cassKeySplit = cassKey.split("\\.");
                    Map cassKeyMap = ((Map) map.getOrDefault(cassKeySplit[0], new HashMap()));
                    map.putIfAbsent(cassKeySplit[0], cassKeyMap);
                    logger.info(
                            "Updating yaml: PriamKey: [{}], Key: [{}], OldValue: [{}], NewValue: [{}]",
                            priamKey,
                            cassKey,
                            cassKeyMap.get(cassKeySplit[1]),
                            cassVal);
                    cassKeyMap.put(cassKeySplit[1], cassVal);
                }
            }
        }
    }
}
