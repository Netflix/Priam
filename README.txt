Priam is a process/tool built for Apache Cassandra to perform the following:
- Token management
- Configuration
- Backup and recovery (Complete and incremental)
- Supports AWS environment

Apache Cassandra is a highly available column oriented database: http://cassandra.apache.org
and Priam is a father of cassandra in greek mythology. 

Priam is actively developed and used at Netflix. 

Features:
- Token management using simpledb (other implementations to follow)
- Support multi-region Cassandra deployment in AWS via public IP.
- Automated security group update in multi-region environment.
- Backup SSTables to S3.
- Backup uses Snappy compression to compress data on fly. They do not disturb file cache and are throttled.
- Pluggable modules for the future enhancements. (supporting for multiple data storage).
- APIs to list and restore backup data.
- Commit log backup (coming upi, stay tuned)
- Scheduler interface for better thread Priority.
