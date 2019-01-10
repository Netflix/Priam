# Changelog
## 2019/01/10 3.1.69 
* (#762) Backup Verification for Backup 2.0. 
* (#762) Restore for Backup 2.0 
* (#762) Some API changes for Snapshot Verification 
* (#762) Remove deprecated code like flush hour or snapshot hour. 

## 2018/11/29 3.1.68 
* (#757) Add new file format (SST_V2) and methods to get/parse remote locations.
* (#757) Upload files from SnapshotMetaService in backup version 2.0, if enabled.
* (#757) Process older SNAPSHOT_V2 at the restart of Priam.

## 2018/10/29 3.1.67
* Bug Fix: SnapshotMetaService can leave snapshots if there is any error. 
* Bug Fix: SnapshotMetaService should continue building snapshot even if an unexpected file is found in snapshot. 
* More cleanup of IConfiguration and moving code to appropriate places. 

## 2018/10/26 3.1.66
* (#744) Aggregate InstanceData in InstanceInfo and pull basic information about running instance from machine itself.  

## 2018/10/17 3.1.65
* (#738) BugFix: Null pointer exception while traversing filesystem. 
* (#736) Google java format validator addition. Use ./gradlew goJF to fix the formatting before sending PR. 
* (#741) Last but not least, a new logo for Priam. 

## 2018/10/10 3.1.64
### New Feature
* (#732) Move forgotten files to `lost+found` directory. This is enabled by configuration `priam.forgottenFileMoveEnabled`. This is disabled by default. 

## 2018/10/05 3.1.63
***WARNING*** THIS IS A BREAKING RELEASE 
### New Feature
* (#722) Restores will be async in nature by default. 
* (#722) Support for async snapshots via configuration - `priam.async.snapshot`. Similar support for async incrementals via configuration - `priam.async.incremental`. 
* (#722) Better metrics for upload and download to/from remote file system. 
* (#722) Better support for include/exclude keyspaces/columnfamilies from backup, incremental backup and restores. 
### Bug fix
* (#722) Metrics are incremented only once and in a central location at AbstractFileSystem. 
* (#722) Remove deprecated AWS API Calls.
### Breaking changes
* (#722) Removal of MBeans to collect metrics from S3FileSystem. They were unreliable and incorrect. 
* (#722) Update to backup configurations :- isIncrBackupParallelEnabled, getIncrementalBkupMaxConsumers, getIncrementalBkupQueueSize. They are renamed to ensure naming consistency. Refer to wiki for more details. 
* (#722) Changes to backup/restore configuration :- getSnapshotKeyspaceFilters, getSnapshotCFFilter, getIncrementalKeyspaceFilters, getIncrementalCFFilter, getRestoreKeyspaceFilter, getRestoreCFFilter. They are now centralized to ensure that we can support both include and exclude keyspaces/CF. Refer to wiki for more details. 

## 2018/10/01 3.1.62
* (#693) Bug fix: If priam has issue while uploading incrementals (when uploading in parallel), and exhaust the retries, it never tries to upload the file again. 

## 2018/09/28 3.1.61
### New Feature
* (#712) Forgotten files: Cassandra has a bug where it can leave forfotten files in data folder. Priam will increment the forgotten file metric, if it figures out such a case, when it takes a full snapshot. 
* (#718) Add local modification time to S3 objects. 
* (#724) Expose priam configuration over HTTP and persist at regular interval (CRON) to local file system for automation/tooling. 

## 2018/08/20 3.1.60
***WARNING*** THIS IS A BREAKING RELEASE 
### New Feature
* (#706) Move to spectator to collect Metrics. This is a breaking change, if you were depending on MetricPublisher to collect metrics. 

## 2018/08/13 3.1.59 
### New Feature
* (#671) Compactions on CRON: Added ability to schedule compactions for columnfamilies on CRON. 
* (#691) Snapshot Meta Service: This will form the basis of backup version 2. This service will run on CRON and will ensure we upload a meta file containing the list of SSTables on disk. 
* (#703) Making post restore hook heartbeat timeout and heartbeat check frequency configurable

## 2018/07/28: 3.1.58
* (#695) Moving restore-finished status update, to after PostRestoreHook execution.

## 2018/07/11: 3.1.57
* (#689) Adding partitioner endpoint to cassadmin resource to get C* partitioner name.

## 2018/06/28: 3.1.54
### Improvements
* (#683) PostRestoreHook logging improvements.


## 2018/06/27: 3.1.53
### New Feature
* (#681) PostRestoreHook changes. Adding ability to call a post restore hook once files get downloaded as part of the restore process.

Following are relevant configurations around post restore hook
- priam.postrestorehook.enabled: indicates if postrestorehook is enabled.
- priam.postrestorehook: contains the command with arguments to be executed as part of postrestorehook. Priam would wait for completion of this hook before proceeding to starting C*.
- priam.postrestorehook.heartbeat.filename: heartbeat file that postrestorehook emits. Priam keeps a tab on this file to make sure postrestorehook is making progress. Otherwise, a new process of postrestorehook would be spawned (upon killing existing process if still exists)
- priam.postrestorehook.done.filename:'done' file that postrestorehook creates upon completion of execution.
- priam.postrestorehook.timeout.in.days:maximum time that Priam should wait before killing the postrestorehook process (if not already complete)

## 2018/06/12: 3.1.52
### Bug Fixes
* (#679) Mark snapshot as a failure if there is an issue with uploading a file. This is to ensure we fail-fast. This is in contrast to previous behavior where snapshot would "ignore" any failures in the upload of a file and mark snapshot as "success". 
         Since it was not truly a "success" marking that as "failure" is the right thing to do. Also, meta.json should really be uploaded in case of "success" and not in case of "failure" as the presence of "meta.json" marks the backup as successful.
         The case for fail-fast: In a scenario where we had an issue say at the start of the backup, it makes more sense to fail-fast then to keep uploading other files (and waste bandwidth and use backup resources). The remediation step for backup failure is anyways to take a full snapshot again.

## 2018/06/07: 3.1.51
### Bug Fixes
* (#678) Change the default location of backup status and downloaded meta.json as part of backup verification

## 2018/03/07: 3.1.50

### New Features
* (#664) Cassandra Process Manager can be configured to gracefully stop using the new 
 `gracefulDrainHealthWaitSeconds` option. If this option set to a positive integer (>=0) then before calling 
 the shutdown script, Priam will fail healthchecks (`InstanceState.isHealthy`) for the configured number of seconds and  then will issue a `nodetool drain` with 30s timeout (since drain can hang), and finally call the provided stop script. By default this is set to `-1` to disable this feature for backwards compatibility. This is useful if you want to gracefully drain cassandra clients off a node before running `drain` (which kills the Native/Thrift server and resets and tcp connections that were established; in flight requests can get   dropped), then running drain to safely stop Cassandra, and then call your stop script. If your service discovery system does not integrate with Priam's health system or your stop script already does all these things then leave this functionality disabled.
* (#664) `/v1/cassadmin/stop` http API call now takes an optional `force` parameter (e.g. `/v1/cassadmin/stop?force=true` which will skip the graceful path for that particular stop; default value is `false`.
* (#650) Enable auth on the jmx port via `jmxUsername` and `jmxPassword` options. By default these are null and not provided.

### Bug Fixes
* (#659) Fix to `Snapshotstatus` to actually contain `bkupMetadata`
* (#661) Update `commons-io`, `aws-java-sdk`, `snakeyaml`

### Breaking changes
* (#664) If you previously implemented `ICassandraProcess` internally the `start` method has been refactored to take a `boolean force` parameter. If you implement this interface you can supply `false` to preserve previous behavior. 

## 2018/02/01: 3.1.48

### Bugs
* Autostart functionality now uses timers instead of ratelimiters so that
  the first autostart does not start until an interval after the first start.

## 2018/01/11: 3.1.47

### New Features
* Cassandra Process Manager and Monitor now record metrics when C* is stopped, started or auto-started with recent autorestart functionality. 
* Location of backup status file is now configurable via configuration `priam.backup.status.location`.
* SDBInstance for token management with default binding to us-east-1 but configurable via `priam.sdb.instanceIdentity.region`.

### Bugs
* Exclude duplicate sl4j module binding.
* Shut down quartz at application stop


## 2017/12/14: 3.1.46

### Bugs
* Gradle 4.4 Support
* Autostart functionality now only sets shouldCassandraBeAlive flag from
  the start api to prevent a race against the stop API in the monitoring
  thread.

## 2017/12/06: 3.1.45

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
a Initial external release
