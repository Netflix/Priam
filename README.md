Overview
========
Priam is a process/tool that runs alongside Apache Cassandra to automate the following:
- Backup and recovery (Complete and incremental)
- Token management
- Configuration
- Support AWS environment

Apache Cassandra is a highly available, column oriented database: http://cassandra.apache.org.

The name 'Priam' refers to King of Troy in Greek mythology, who the father of Cassandra. 

Priam is actively developed and used at Netflix. 

Features:
- Token management using SimpleDB
- Support multi-region Cassandra deployment in AWS via public IP.
- Automated security group update in multi-region environment.
- Backup SSTables from local ephemeral disks to S3.
- Uses Snappy compression to compress backup data on the fly. 
- Backup throttling
- Pluggable modules for future enhancements (support for multiple data storage).
- APIs to list and restore backup data.
- REST APIs for backup/restore and other operations

Compatibility:
The master branch of Priam is compatibile with Cassandra 1.1. If you are using Cassandra 1.0, please use the 1.0 branch of Priam.


Setup
=====
Coming soon...