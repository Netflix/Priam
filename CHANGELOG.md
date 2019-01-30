# Changelog

## 2019/01/30 3.11.39
(#765) Add metrics on CassandraConfig resource calls
(#768) Support configure/tune complex parameters in cassandra.yaml 
(#770) Add Cass SNAPSHOT JMX status, snapshot version, last validated timestamp. Changes to Servlet API and new APIs. 

## 2019/01/11 3.11.38
(#761) Add new file format (SST_V2) and methods to get/parse remote locations.
(#761) Upload files from SnapshotMetaService in backup version 2.0, if enabled.
(#761) Process older SNAPSHOT_V2 at the restart of Priam.
(#767) Backup Verification for Backup 2.0.
(#767) Restore for Backup 2.0
(#767) Some API changes for Snapshot Verification
(#767) Remove deprecated code like flush hour or snapshot hour.

## 2018/10/29 3.11.37
* Bug Fix: SnapshotMetaService can leave snapshots if there is any error. 
* Bug Fix: SnapshotMetaService should continue building snapshot even if an unexpected file is found in snapshot. 
* More cleanup of IConfiguration and moving code to appropriate places. 

## 2018/10/26 3.11.36
* (#747) Aggregate InstanceData in InstanceInfo and pull information about running instance 

## 2018/10/17 3.11.35
* (#739) BugFix: Null pointer exception while traversing filesystem. 
* (#737) Google java format validator addition. Use ./gradlew goJF to fix the formatting before sending PR. 
* (#740) Last but not least, a new logo for Priam. 

## 2018/10/08 3.11.33
***WARNING*** THIS IS A BREAKING RELEASE 
### New Feature
* (#731) Restores will be async in nature by default. 
* (#731) Support for async snapshots via configuration - `priam.async.snapshot`. Similar support for async incrementals via configuration - `priam.async.incremental`. 
* (#731) Better metrics for upload and download to/from remote file system. 
* (#731) Better support for include/exclude keyspaces/columnfamilies from backup, incremental backup and restores. 
* (#731) Expose priam configuration over HTTP and persist at regular interval (CRON) to local file system for automation/tooling. 
### Bug fix
* (#731) Metrics are incremented only once and in a central location at AbstractFileSystem. 
* (#731) Remove deprecated AWS API Calls.
### Breaking changes
* (#731) Removal of MBeans to collect metrics from S3FileSystem. They were unreliable and incorrect. 
* (#731) Update to backup configurations :- isIncrBackupParallelEnabled, getIncrementalBkupMaxConsumers, getIncrementalBkupQueueSize. They are renamed to ensure naming consistency. Refer to wiki for more details. 
* (#731) Changes to backup/restore configuration :- getSnapshotKeyspaceFilters, getSnapshotCFFilter, getIncrementalKeyspaceFilters, getIncrementalCFFilter, getRestoreKeyspaceFilter, getRestoreCFFilter. They are now centralized to ensure that we can support both include and exclude keyspaces/CF. Refer to wiki for more details. 

## 2018/10/02 3.11.32
* (#727) Bug Fix: Continue uploading incrementals when parallel incrementals is enabled and file fails to upload. 
* (#718) Add last modified time to S3 Object Metadata. 

## 2018/09/10 3.11.31 
* (#715) Bug Fix: Fix the bootstrap issue. Do not provide yourself as seed node if cluster is already up and running as it will lead to data loss. 

## 2018/08/20 3.11.30
***WARNING*** THIS IS A BREAKING RELEASE
### New Feature
* (#707) Move to spectator to collect Metrics. This is a breaking change, if you were depending on MetricPublisher to collect metrics. 

## 2018/08/13 3.11.29
### New Feature
* (#705) Snapshot Meta Service: This will form the basis of backup version 2. This service will run on CRON and will ensure we upload a meta file containing the list of SSTables on disk. 

## 2018/08/09 3.11.28
### New Feature
* (#699) Compactions on CRON: Added ability to schedule compactions of columnfamilies on CRON. 
* (#699) Move to cassandra-all:3.0.17
### Deprecation 
* (#699) Deprecate flush hour and backup hour. 

## 2018/08/03: 3.11.27
### Improvements
* (#702) Making post restore hook heartbeat timeout and heartbeat check frequency configurable

## 2018/07/28: 3.11.26
### Improvements
* (#694) Moving restore-finished status update, to after PostRestoreHook execution.

## 2018/07/11: 3.11.25
### Improvements
* (#690) Adding partitioner endpoint to cassadmin resource to get C* partitioner name.

## 2018/06/28: 3.11.23
### Improvements
* (#684) PostRestoreHook logging improvements.


## 2018/06/27: 3.11.22
### New Feature
* (#682) PostRestoreHook changes. Adding ability to call a post restore hook once files get downloaded as part of the restore process.

Following are relevant configurations around post restore hook
- priam.postrestorehook.enabled: indicates if postrestorehook is enabled.
- priam.postrestorehook: contains the command with arguments to be executed as part of postrestorehook. Priam would wait for completion of this hook before proceeding to starting C*.
- priam.postrestorehook.heartbeat.filename: heartbeat file that postrestorehook emits. Priam keeps a tab on this file to make sure postrestorehook is making progress. Otherwise, a new process of postrestorehook would be spawned (upon killing existing process if still exists)
- priam.postrestorehook.done.filename:'done' file that postrestorehook creates upon completion of execution.
- priam.postrestorehook.timeout.in.days:maximum time that Priam should wait before killing the postrestorehook process (if not already complete)


## 2018/06/12: 3.11.21
### Bug Fixes
* (#679) Mark snapshot as a failure if there is an issue with uploading a file. This is to ensure we fail-fast. This is in contrast to previous behavior where snapshot would "ignore" any failures in the upload of a file and mark snapshot as "success". 
         Since it was not truly a "success" marking that as "failure" is the right thing to do. Also, meta.json should really be uploaded in case of "success" and not in case of "failure" as the presence of "meta.json" marks the backup as successful.
         The case for fail-fast: In a scenario where we had an issue say at the start of the backup, it makes more sense to fail-fast then to keep uploading other files (and waste bandwidth and use backup resources). The remediation step for backup failure is anyways to take a full snapshot again.

## 2018/06/07: 3.11.20
### Bug Fixes
* (#678) Change the default location of backup status and downloaded meta.json as part of backup verification

## 2018/04/05: 3.11.19

### New Features
* (#665) Cassandra Process Manager can be configured to gracefully stop using the new
 `gracefulDrainHealthWaitSeconds` option. If this option set to a positive integer (>=0) then before calling
 the shutdown script, Priam will fail healthchecks (`InstanceState.isHealthy`) for the configured number of seconds and  then will issue a `nodetool drain` with 30s timeout (since drain can hang), and finally call the provided stop script. By default this is set to `-1` to disable this feature for backwards compatibility. This is useful if you want to gracefully drain cassandra clients off a node before running `drain` (which kills the Native/Thrift server and resets and tcp connections that were established; in flight requests can get   dropped), then running drain to safely stop Cassandra, and then call your stop script. If your service discovery system does not integrate with Priam's health system or your stop script already does all these things then leave this functionality disabled.
* (#665) `/v1/cassadmin/stop` http API call now takes an optional `force` parameter (e.g. `/v1/cassadmin/stop?force=true` which will skip the graceful path for that particular stop; default value is `false`.
* (#650) Enable auth on the jmx port via `jmxUsername` and `jmxPassword` options. By default these are null and not provided.

### Bug Fixes
* (#662) Update `commons-io`, `aws-java-sdk`, `snakeyaml`

### Breaking changes
* (#665) If you previously implemented `ICassandraProcess` internally the `start` method has been refactored to take a `boolean force` parameter. If you implement this interface you can supply `false` to preserve previous behavior.

## 2018/02/13: 3.11.18

### Bug Fixes
* (#660) Fix to `Snapshotstatus` to actually contain `bkupMetadata`

## 2018/02/01: 3.11.17

### New Features
* (#639) bakup.status is now a variable
* (#647) SDB clients standardized

### Bugs
* (#658) Autostart functionality now uses timers instead of ratelimiters so that
  the first autostart does not start until an interval after the first start.
* (#632) Duplicate slf4j bindings excluded
* (#643) Gracefully shut down quartz

## 2017/12/14: 3.11.16

### Bugs
* Gradle 4.4 Support
* Autostart functionality now only sets shouldCassandraBeAlive flag from
  the start api to prevent a race against the stop API in the monitoring
  thread.

## 2017/12/06: 3.11.15

### Bugs
* None

### New Features
* Priam will now automatically restart Cassandra if it fails. If you use
  Priam to stop Cassandra (via the API) it will not automatically restart
  Cassandra until a subsequent start via the API. You can control this
  via the ``priam.remediate.dead.cassandra.rate`` configuration option. If
  negative it disables auto-remediation, if zero it immediately auto-remediates
  on any failure, and if a positive integer the auto-remediation waits for
  that number of seconds between restarts. The default is 360 seconds
  (one hour).

### Breaking Changes
* None

## Previous changelog
1.1
- Support for cassandra 1.1
- Support to publish cassandra metrics (TODO)

1.0
- Support for cassandra 1.0
- Incremental restores
- Multiple bug fixes

0.0.5
- Initial external release
