# Changelog

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
