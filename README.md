# Priam

2.x status:
[![Build Status](https://travis-ci.org/Netflix/Priam.svg?branch=2.x)](https://travis-ci.org/Netflix/Priam)

3.x status:
[![Build Status](https://travis-ci.org/Netflix/Priam.svg?branch=3.x)](https://travis-ci.org/Netflix/Priam)

Priam is a process/tool that runs alongside Apache Cassandra to automate the following:
- Backup and recovery (Complete and incremental)
- Token management
- Seed discovery
- Configuration
- Support AWS environment

Apache Cassandra is a highly available, column oriented database: http://cassandra.apache.org.

The name 'Priam' refers to the King of Troy in Greek mythology, who was the father of Cassandra. 

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

Compatibility Matrix
------------------------

Priam versions and their compatibility with Cassandra versions in explained in matrix below

|Priam Branch (Release) |Cassandra Version |Description |
| :------- | :---- | :--- |
|3.11| C* 3.11 | Currently it supports Apache Cassandra 3.x |
|3.x| C* 2.1.x | Any minor version of C* 2.1.x|
|2.x| C* 2.0.x| Any minor version of C* 2.0.x is supported. No longer under active development|
|1.2.x | C* 1.2 only | Any minor version of C* 1.2.x is supported. No longer under active development |
|1.1| C* 1.1 only| No longer under active devleopment|
|master |C* 1.2 - 2.0| Currently it supports C* v1.2 and 2.0. No longer under active development|
