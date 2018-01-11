# Changelog

## 2018/01/11: 3.1.47

# New Features
* Cassandra Process Manager and Monitor now record metrics when C* is stopped, started or auto-started with recent autorestart functionality. 
* Location of backup status file is now configurable via configuration `priam.backup.status.location`.
* SDBInstance for token management with default binding to us-east-1 but configurable via `priam.sdb.instanceIdentity.region`.

# Bugs 
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
- Initial external release
