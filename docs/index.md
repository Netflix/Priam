![alt text](../images/priam.png "Priam Logo")


# About


Priam is a process/tool that runs alongside [Apache Cassandra] (http://cassandra.apache.org), a highly available, column-oriented database. Priam automates following tasks:

* Backup and recovery (Complete and incremental)
* Token management
* Seed discovery
* Configuration

The name 'Priam' refers to the King of Troy in Greek mythology, who was the father of Cassandra. Priam is actively developed and used at Netflix since mid 2011.

## Features
* Token management using SimpleDB
* Support multi-region Cassandra deployment in AWS via public IP.
* Automated security group update in multi-region environment.
* Backup SSTables from local ephemeral disks to S3.
* Uses Snappy compression to compress backup data on the fly.
* Backup throttling
* Pluggable modules for future enhancements (support for multiple data storage).
* REST APIs for backup/restore and other operations
* REST APIs for validating backups. 
* Monitor health of Cassandra and auto-remediate common issues. 

## Requirements
* Cloud support: AWS only
* Requires deployment of EC2 Instances behind an Auto Scaling Group(ASG). Ideally one ASG per datacenter.
* Supports one token per EC2 instance (no support for vnodes yet)

## Compatibility


|Priam Branch|Cassandra Version|Description|Javadoc|
|--------|:-------|:--------|:--------|
|[4.x](https://github.com/Netflix/Priam/tree/4.x)       |C* 4.x                      | Alpha: Currently it supports Apache C* 4.x|[link](https://www.javadoc.io/doc/com.netflix.priam/priam/4.0.0-alpha7)
|[3.11](https://github.com/Netflix/Priam/tree/3.11)                 | C* 3.x                     | Currently it supports Apache C* 3.x |[link](https://www.javadoc.io/doc/com.netflix.priam/priam/3.11.35)
|[3.x](https://github.com/Netflix/Priam/tree/3.x)                  | C* 2.1.x                  | Any minor version of Apache C* 2.1.x and DSE |[link](https://www.javadoc.io/doc/com.netflix.priam/priam/3.1.65)

## Authors 
1. Arun Agrawal @arunagrawal84
2. Joseph Lynch @jolynch
3. Vinay Chella @vinaykumarchella

## License
Copyright 2011-2018 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at

(http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

A Netflix Original Production

[Netflix OSS](http://netflix.github.io/#repo) | [Tech Blog](https://medium.com/netflix-techblog) | [Twitter @NetflixOSS](https://twitter.com/NetflixOSS) | [Jobs](https://jobs.netflix.com/)
