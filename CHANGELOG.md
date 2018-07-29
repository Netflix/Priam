# Changelog

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
