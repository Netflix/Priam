Priam is a Backup/Recovery, Configuration and Token Management tool for Apache Cassandra.

Apache Cassandra is a highly available column oriented database: http://cassandra.apache.org
Priam is a father of cassandra in greek mythology. 

Priam is currently in use at Netflix. Issues generally are fixed as quickly as they are discovered and releases done frequently.

Features:
Backups will not disturb the file cache.
Snappy Compression on the backups
Pluggable modules for the future enhancements. (supporting for multiple data storage).
Recovery with one API call.
New release supports Multi region via Public IP (Automated update for EC2, security groups)
Commit log backup decoupling EBS volume dependencies.
Scheduler interface for better thread Priority.
Centralized Configuration Management
Decentralized Token Management (to avoid doing manual allocation - automation)
