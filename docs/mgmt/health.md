# Health

Priam being the sidecar to Cassandra, monitors the health of local Cassandra. It allows user to poll for the health of Cassandra and take actions. 

Priam also allows user to configure to auto-start Cassandra, when Priam detects it is down. This is helpful so when Cassandra gets Out Of Memory (OOM) for any reason, Priam can simply re-start the process reducing the time to recovery. 

## Configuration
**_priam.remediate.dead.cassandra.rate_**: int representing how often (in seconds) Priam should auto-remediate Cassandra process
                      crash If zero, Priam will restart Cassandra whenever it notices it is crashed If a
                      positive number, Priam will restart cassandra no more than once in that number of
                      seconds. For example a value of 60 means that Priam will only restart Cassandra once per
                      60 seconds If a negative number, Priam will not restart Cassandra due to crash at all. Default: `3600` (once per hour)


 