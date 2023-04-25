# Changelog
## 2023/4/25 3.1.124
*V1 Backups will be removed in the next release*
Increment cross regional duplicate tokens to replicate the policy we have been applying manually. (#1048)
Fix Github CI by explicitly creating necessary directories. (#1045)
Always TTL backups. (#1038)
Fix backup verification race condition causing missing notifications (#1034)
Reveal hook to allow operators to restore just to the most recent snapshot (#1035)
Reveal property to enable auto_snapshot. (#1031)

## 2023/2/27 3.1.123
Switch from com.google.inject to JSR-330 javax.inject annotations for better compatibility (#1029)

## 2023/1/21 3.1.122
Log backup failures rather than ignoring them. (#1026) 
Remove redundant interfaces and swap log and notification lines in the case of backup failure. (#1023) 
Use synchronized list for thread-safety (#1022) 

## 2022/11/15 3.1.121
(#1013) Create operator-specifiable time such that if a backup file was written before then it is automatically compressed using SNAPPY before upload.

## 2022/10/10 3.1.120
(#1011) Allow user to specify additional message attributes to send in SNS backup notifications. 
(#998) Identify incrementals, optionally skip metafile compression, and upload data files last.
(#1008) Pass millis to sleep instead of seconds.

## 2022/09/15 3.1.119
(#1004) RandomPartitioner creates tokens that don't always fit into a long so we use BigInteger to store and compare them.

## 2022/09/14 3.1.118
(#996) Revert "CASS-2805 Add hook to optionally skip deletion for files added within a certain window. (#993)"
(#1000) Use IP to get gossip info rather than hostname.
(#1003) Spread deletes over the interval depending on where the node is in the ring.
(#967) Remove UpdateSecuritySettings

## 2022/06/26 3.1.117
*UpdateSecuritySettings will be removed in the next release*
(#993) Add hook to optionally skip deletion for files added within a certain window.

## 2022/06/13 3.1.116
(#987) Remove unsupported method and subtle logic change from recent backports

## 2022/06/09 3.1.115
*UpdateSecuritySettings will be removed in the next release*
(#975 backport) Dynamic Rate Limiting of Snapshots.
(#985 backport) Optionally Add Content-MD5 header to uploads of files smaller than the backupChunkSize.

## 2022/01/25 3.1.114
(#980) Cache region in AWSInstanceInfo to avoid throttling.
NOTE: UpdateSecuritySettings will be deprecated in an upcoming release.

## 2022/01/24 3.1.113
(#978) Quick fix that re-enables local bootstrapping
NOTE: UpdateSecuritySettings will be deprecated in an upcoming release.

## 2022/01/04 3.1.112
(#976) Use IMDSV2 to get instance metadata

## 2021/08/31 3.1.111
(#972) Throw in case of gossip mismatch when determining replace_ip in assigned token case
(#969) Make getPublicIP and getPublicHostname fail elegantly when those attributes are absent.
(#968) Delete empty incremental backup files rather than throwing when we try to rate limit them.

## 2021/06/11 3.1.110
(#953) Fixing integer overflow problem in restore

## 2021/06/10 3.1.109
(#951) Removing instances of methods that don't exist in guava 19.0

## 2021/06/09 3.1.108
(#950) Pinning guava to 19.0 which is the max 2.1 Cassandra can tolerate.

## 2021/06/07 3.1.107
(#949) Reverting back to previous behavior of omitting milliseconds in backup last modified times.

## 2021/05/31 3.1.106
(#947) Optionally skip compression when uploading backups.
(#946) Allow operators to restrict ingress rules to public IPs exclusively.
(#936) Optionally check to ensure Thrift server is listening on rpc_port before claiming it is healthy.

## 2021/05/12 3.1.105
(#939) Improve the configurability of setting ingress rules.

## 2021/03/23 3.1.104
Bugfix: Revert adding role_manager to cassandra 2.1 yaml due to incompatibility

## 2021/03/17 3.1.103
(#923) Store private ips in the token database when using GPFS. Plus substantial refactoring of token generation logic.

## 2020/09/30 3.1.100
(#907, #909) Stop explicitly filtering OpsCenter keyspace when backing up. Remove more noisy log statements.

## 2020/09/08 3.1.99
(#903) Remove noisy log statements from CassandraAdmin.

## 2020/08/11 3.1.98
(#900) Throw when gossip unanimously says token is already owned by a live node.

## 2020/07/15 3.1.97
(#898) Make BackupVerificationTask log and emit when there is no verified backup within SLO. Cease requiring the backup to be fully in S3.

## 2020/07/13 3.1.96
(#894) Fix the inferTokenOwnership information. This will provide all the details to the caller method so they can make decision rather than throwing any exception. 

## 2020/07/02/ 3.1.95
(#890) Adding an exception in the replace-ip path when a node attempts to bootstrap to an existing token because of a stale state. 

## 2020/06/29 3.1.94
(#888) Porting PropertiesFileTuner to the 3.x branch.

## 2020/05/19 3.1.93
Re-releasing 3.1.92

## 2020/05/19 3.1.92
(#877) Fixing BackupServletV2 endpoints that were broken because of an underlying dependency change from the release 3.1.89.

## 2020/05/18 3.1.91
(#871) Fixing PriamConfig endpoints that were broken because of an underlying dependency change from the last release.

## 2020/05/05 3.1.90
This is a re-release of 3.1.89 since that release failed due to a test that failed because of a concurrent execution on another release train.

## 2020/05/05 3.1.89
(#859, #863) Fixing the bug in the backup verification strategy to only page when there is no valid backup in the specified date range (SLO window) And also disable lifecyle rule for backup if backup v1 is disabled.

## 2020/04/22 3.1.88
(#848) Modifying the backup verification strategy to verify all unverified backups in the specified date range vs the old implementation that verified the latest backup in the specified date range.

## 2020/02/21 3.1.87
(#842, #839) Implementation of a filter for Backup Notification. The filter can be controlled using the configuration "priam.backupNotifyComponentIncludeList"

## 2019/10/24 3.1.86
(#835) Move flush and compactions to Service Layer. This allows us to "hot" reload the jobs when configurations change.
(#835) Send SNAPSHOT_VERIFIED message when a snapshot is verified and ready to be consumed by downward dependencies.

## 2019/08/23 3.1.85
(#832) Travis build fails for oraclejdk8. Migration to openjdk8

## 2019/08/23 3.1.84
(#827) Removing functionality of creating incremental manifest file in backup V1 as it is not used. 
(#827) Bug fix: When meta file do not exist for TTL in backup v2 we should not be throwing NPE. 
(#827) Bug fix: Fix X-Y-Z issue using gossip status information instead of gossip state. Note that gossip status is (JOINING/LEAVING/NORMAL) while gossip state is (UP/DOWN). Gossip state is calculated individually by all the Cassandra instances using gossip status. 

## 2019/06/07 3.1.83
(#825): Rollback the fixes to use Gossip info while grabbing dead and pre-assigned tokens. Gossip info doesn't not reflect the correct cluster state always. A node marked with status as NORMAL in the Gossip info could actually be down. This can be checked using nt ring. This change will unblock the nodes from joining the ring.

## 2019/05/28 3.1.82
(#821): Backport Configuring JVM Options for Cassandra using jvm.options if your Cassandra supports jvm.options, you can enable it by setting Priam.jvm.options.supported, then the options are the same as 3.11
(#822): Use replace_address instead of replace_address_first_boot. replace_address always try to bootstrap Cassandra in replace mode even when the previous bootstrap is successful. replace_address_first_boot tries to bootstrap normally if the node already bootstrapped successfully.

## 2019/05/14 3.1.81
(#817) Changing the list in TokenRetrievalUtils to use wildcards.

## 2019/05/13 3.1.80
(#814) Priam will check Cassandra gossip information while grabbing pre-assigned token to decide if it should start Cassandra in bootstrap mode or in replace mode.
(#814) At most 3 random nodes are used to get the gossip information.
(#814) Moved token owner inferring logic based on Cassandra gossip into a util class.
(#814) Refactored InstanceIdentity.init() method.

## 2019/04/29 3.1.79
(#810) Update the backup service based on configuration changes.
(#811) Expose the list of files from backups as API call.
(#808) Run TTL for backup based on a simple timer to avoid S3 delete API call throttle.
(#808) API to clear the local filesystem cache.
(#811) Bug fix: Increment backup failure metric when no backup is found.
(#808) Bug fix: No backup verification job during restore.

## 2019/03/19 3.1.78
(#795) Fix X->Y->Z issue. Replace nodes when gossip actually converges.

## 2019/03/13 3.1.77
(#801) Write-thru cache in AbstractFileSystem.
(#801) Take care of issue - C* snapshot w.r.t. filesystem is not "sync" in nature.

## 2019/03/05 3.1.76
(#799) Fix for forgotten file
(#799) Use older API for prefix filtering (backup TTL), if prefix is available.
(#799) Send notifications only when we upload a file.

## 2019/02/27 3.1.75
(#792) S3 - BucketLifecycleConfiguration has `prefix` method removed from latest library.

## 2019/02/27 3.1.74
(#790) BackupServlet had an API call of backup status which was producing output which was not JSON.

## 2019/02/15 3.1.73
(#773): BackupVerificationService
(#779): Put a cache for the getObjectExist API call to S3. This will help keep the cost of this call at bay.
(#779): Put a rate limiter for getObjectExist API call to S3 so we can limit the no. of calls.
(#782): Provide an override method to force Priam to replace a particular IP.

## 2019/02/08 3.1.72
(#778)Do not check existence of file if it is not SST_V2. S3 may decide to slow down and throw an error. Best not to do s3 object check (API) if it is not required.

## 2019/02/07 3.1.71
(#774) Do not throw NPE when no backup is found for the requested date.

## 2019/01/30 3.1.70
(#765) Add metrics on CassandraConfig resource calls
(#768) Support configure/tune complex parameters in cassandra.yaml 
(#771) Add Cass SNAPSHOT JMX status, snapshot version, last validated timestamp. Changes to Servlet API and new APIs. 

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
