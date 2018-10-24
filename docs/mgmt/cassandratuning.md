# Cassandra Tuning
Priam tunes common cassandra configurations out of the box. Priam traditionally had support to tune [Datastax Cassandra](https://www.datastax.com/products/datastax-enterprise). 
With Datastax forking the Cassandra, Priam has stopped maintaining the DSE tuners since 3.11 branch of Priam. **Note**: DSE tuners will not be available going forward. 

Apache Cassandra 3.0.x (and above) have additional files that can be configured like [jvm.options](#jvm options tuning).  

## Seed Provider
Priam provides its own Seed Provider for Cassandra: `NFSeedProvider`. This allows Cassandra to get list of `seed` nodes from Priam at every startup. 
Priam sends an updated list of instances which can be used as `seed` for Cassandra to `bootstrap`. Priam manages the `bootstrap` process of cassandra based on: 
* This is a new cluster
* Replacement of the instance
* Doubling of Cassandra cluster
* Restore mode. 

1. **_priam.seed.provider_**: Seed provider to be used to determine the `seed` nodes for an instance to bootstrap. Default: `com.netflix.priam.cassandra.extensions.NFSeedProvider`. 

## Configurations

#### Ports
1. **_priam.storage.port_**: Cassandra storage/cluster communication port. Default: `7000`. 
1. **_priam.ssl.storage.port_**: Cassandra SSL enabled storage/cluster communication port. Default: `7001`. 
1. **_priam.thrift.port_**: Cassandra thrift(pre-CQL) port for clients to connect to. Default: `9160`. 
1. **_priam.nativeTransport.port_**: Cassandra native transport port for clients to connect to (CQL). Default: `9042`. 
1. **_priam.jmx.port_**: Cassandra JMX port. Priam uses JMX to connect to local Cassandra to automate various management functions. Default: `7199`

#### Cassandra
1. **_priam.thrift.enabled_**: Should thrift protocol be enabled on cassandra? This is deprecated in Apache Cassandra going forward (4.x and above). Default: `true` in `3.x` branch, else `false`. 
1. **_priam.nativeTransport.enabled_**: Should native protocol be enabled on cassandra. This is preferred approach. Default: `true`
1. **_priam.endpoint_snitch_**: Snitch to be used by Cassandra to identify the other Cassandra instances. This depends on the environment and deployment topology of Cassandra. Example: for multi-region cluster this should be `EC2MultiRegionSnitch`. Default: `org.apache.cassandra.locator.Ec2Snitch`. This assumes an AWS hosted, single region cluster deployment. 
1. **_priam.compaction.throughput_**: Compaction throughput allowed in MB/sec for Cassandra. Default: `8`
1. **_priam.partitioner_**: Partitioner algorithm to be used by Cassandra to hash the data. Default: `org.apache.cassandra.dht.RandomPartitioner` for `3.x` and `3.11` branch. **Note**: `org.apache.cassandra.dht.Murmur3Partitioner` has better performance and will be default going forward. 
1. **_priam.streaming.throughput.mb_**: Streaming throughput outbound in MB/sec for Cassandra. Default: `400`. 
1. **_priam.internodeCompression_**: Compression to use by Cassandra while communicating? Allowed values are - all, dc, none. Default: `all`. 
1. **_priam.dsnitchEnabled_**: Enable dynamic snitch while serving the traffic? Default: `true`. 
1. **_priam.tombstone.warning.threshold_**: Cassandra will log warning messages in cassandra logs if a read encounters more than this value of tombstones. Default: `1000`. 
1. **_priam.tombstone.failure.threshold_**: Cassandra should fail the read if it encounters more than this value of tombstones in a single read operation. Default: `100000`.
1. **_priam.streaming.socket.timeout.ms_**: Streaming socket timeout for Cassandra in milliseconds. Default: `86400000` (1 day). 
1. **_priam.compaction.large.partition.warn.threshold_**: Log warning message in Cassandra logs if it encounters [large partitions](https://academy.datastax.com/units/physical-partition-size?resource=ds220-data-modeling) more than this value (in MB) during compaction. Default: `100`. 

#### Cassandra Directory Configurations
1. **_priam.cass.home_**: Directory location of the cassandra home. Default: `\etc\cassandra`. 
1. **_priam.cache.location_**: Directory location of the cassandra cache location. Default: `\var\lib\cassandra\saved_caches`. 
1. **_priam.commitlog.location_**: Directory location of the cassandra commitlog. Ensure Priam has read/write permissions to this folder. Default: `\var\lib\cassandra\commitlog`.
1. **_priam.data.location_**: Directory location of the cassandra data folder. Ensure Priam has read/write permissions to this folder. Default: `\var\lib\cassandra\data`.
1. **_priam.logs.location_**: Directory location of the cassandra logs. Default: `\var\lib\cassandra\logs`.
1. **_priam.cass.startscript_**: Location (with parameters) of the cassandra start script. Default: `\etc\init.d\cassandra start`
1. **_priam.cass.stopscript_**: Location (with parameters) of the cassandra stop script. Default: `\etc\init.d\cassandra stop`

#### Cassandra JVM Configurations
1. **_priam..heap.size.$INSTANCETYPE_**
1. **_priam.heap.newgen.size.$INSTANCETYPE_**
1. **_priam.direct.memory.size.$INSTANCETYPE_**

## JVM Options Tuning 
Cassandra 3.0.x added a new way to configure heap sizes and pass other JVM parameters (via jvm.options). Priam now supports configuring common options like heap setting and choosing Garbage Collection type (G1GC/CMS) natively. Default being CMS. It logs jvm.options after tuning them.
#### Configuration
1. **_priam.jvm.options.location_**: The file mentioned by this is used as a template and the final location where ```jvm.options``` is read and written so Cassandra can pick it for its use. Default value is ```{$CASS_HOME}/conf/jvm.options```. Note that ```{$CASS_HOME}``` can be configured by using **_priam.cass.home_**. 
2. **_priam.gc.type_**: This is used to configure the garbage collection type for Cassandra to use. The value is an enum of _G1GC_ or _CMS_. The default value is ```CMS```. NOTE: This only _comments_ or _uncomments_ any configuration mentioned in provided _jvm.options_ file. 
3. **_priam.jvm.options.upsert_**: This configuration is comma separated list of JVM options to be appended or updated (change default value) mentioned in ```jvm.options```. Note that JVM parameters are case-sensitive and thus this configuration can only exclude JVM parameters if case match. Example: ```-Dsample=1,-Dsample2,-Xmn20G```
4. **_priam.jvm.options.exclude_**: This configuration is comma separated list of JVM options to be excluded which are mentioned in ```jvm.options```. Note that JVM parameters are case-sensitive and thus this configuration can only exclude JVM parameters if case match. Exclude list is always applied after **_priam.jvm.options.upsert_**. Example: ```-XX:+PrintHeapAtGC,-XX:+UseParNewGC```

## Security Configurations
Priam allows to manage Cassandra security features like authentication, authorization, client SSL etc. Please refer to [doc](faq/enable_auth) to see how to enable authorization and authentication in Cassandra. 

Priam uses `local` JMX Connection to connect to local Cassandra to automate various cluster management operations. Priam allows these JMX connections to be protected via username/password for extra security. 

#### Configurations
1. **_priam.client.sslEnabled_**: Use SSL when clients connect to Cassandra. Default: `false`. 
1. **_priam.internodeEncryption_**: Use encryption when cassandra connects to other instances. Default: `none`
1. **_priam.jmx.username_**: Username to use when connecting to local cassandra. Default: `<empty>`. 
1. **_priam.jmx.password_**: Password to use when connecting to local cassandra. Default: `<empty>`. 
1. **_priam.jmx.remote.enable_**: Enable the local JMX connection to be available remotely. Enabling this is not recommended as it poses security threat. Default: `false`. 