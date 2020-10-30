<h1 align="center">
  <img src="images/priam.png" alt="Priam Logo" />
</h1>

<div align="center">

[Releases][release]&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;[Documentation][wiki]&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;

[![Build Status][img-travis-ci]][travis-ci]

</div>

## Important Notice
* Priam 3.x branch supports Cassandra 2.x (2.0.x and 2.1.x). 

## Table of Contents
[**TL;DR**](#tldr)

[**Features**](#features)

[**Compatibility**](#compatibility)

[**Installation**](#installation)

**Additional Info**
  * [**Cluster Management**](#clustermanagement)
  * [**Changelog**](#changelog)


## TL;DR
Priam is a process/tool that runs alongside Apache Cassandra to automate the following:
- Backup and recovery (Complete and incremental)
- Token management
- Seed discovery
- Configuration
- Support AWS environment

Apache Cassandra is a highly available, column oriented database: http://cassandra.apache.org.

The name 'Priam' refers to the King of Troy in Greek mythology, who was the father of Cassandra.

Priam is actively developed and used at Netflix.

## Features
- Token management using SimpleDB
- Support multi-region Cassandra deployment in AWS via public IP.
- Automated security group update in multi-region environment.
- Backup SSTables from local ephemeral disks to S3.
- Uses Snappy compression to compress backup data on the fly.
- Backup throttling
- Pluggable modules for future enhancements (support for multiple data storage).
- APIs to list and restore backup data.
- REST APIs for backup/restore and other operations

## Compatibility
See [Compatibility](http://netflix.github.io/Priam/#compatibility) for details.


## Installation
See [Setup](http://netflix.github.io/Priam/latest/mgmt/installation/) for details. 


## Cluster Management
Basic configuration/REST API's to manage cassandra cluster. See [Cluster Management](http://netflix.github.io/Priam/latest/management/) for details. 
## Changelog
See [CHANGELOG.md](CHANGELOG.md)

<!-- 
References
-->
[release]:https://github.com/Netflix/Priam/releases/latest "Latest Release (external link) ➶"
[wiki]:http://netflix.github.io/Priam/
[repo]:https://github.com/Netflix/Priam
[img-travis-ci]:https://travis-ci.com/Netflix/Priam.svg?branch=3.x
[travis-ci]:https://travis-ci.com/Netflix/Priam

