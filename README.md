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

Creating the Artifacts
----------------------
Maven will create two artifacts that will need to be installed on every Cassandra node in your ring.  One will be installed as
a plugin to Cassandra itself to provide a custom SeedProvider and a wrapper Daemon.  The other will be a self-contained executable
jar web container (Jetty + Jersey via DropWizard) that will run on every node.  The web container will expose a bunch of REST
servlets used to manage and monitor your cluster.

    $ mvn clean install -DskipTests -P assemble

The "-P assemble" maven profile instructs maven to build a shaded (a.k.a. "fat") executable jar so that there's no need to manage
any other classpath dependencies at runtime.  The artifacts that you will need are:

    target/original-priam-x.y-SNAPSHOT.jar   -->  Cassandra "plugin" jar
    target/priam-x.y-SNAPSHOT.jar            -->  web container "fat" jar


Installing Cassandra
--------------------
I am assuming you already have this part figured out, but these steps are useful for just starting up a real barebones testing
cluster.  On a RHEL-based system, you can use the following:

    # Define DataStax Yum Repo
    cat > /etc/yum.repos.d/datastax.repo <<DSREPO
    [datastax]
    name=DataStax community repo
    baseurl=http://rpm.datastax.com/community/
    failovermethod=priority
    enabled=1
    gpgcheck=0
    DSREPO

    # Install Cassandra
    yum -y install apache-cassandra11


Installing and Configuring Priam
--------------------------------
You'll need to copy a few files to each Cassandra node.  Ensure that the files are owned by the same user that will run Cassandra
(that's just the "cassandra" user if you used the DataStax RPM to install Cassandra)

    target/original-priam-x.y-SNAPSHOT.jar   -->  /usr/share/cassandra/lib
    target/priam-x.y-SNAPSHOT.jar            -->  /usr/share/cassandra
    src/main/resources/conf/priam.yaml       -->  /usr/share/cassandra

Open the /usr/share/cassandra/priam.yaml file that you just copied and make any edits to the settings that you desire.  The primary
setting you'll want to change is the "cassandra.clusterName" property, although this isn't strictly necessary just to evaluate Priam.

Change the Cassandra startup script's (/usr/sbin/cassandra) main class:

    $ sed -i '' 's/org.apache.cassandra.thrift.CassandraDaemon/com.netflix.priam.cassandra.NFThinCassandraDaemon/g' /usr/sbin/cassandra

AWS Account Setup
-----------------
There are a few things that need to be set up in your AWS account in order to use Priam before it can be started.

* S3Bucket
  - Unless you changed the "backup.s3BucketName" property in the priam.yaml, you'll need an s3 bucket called "cassandra-archive" in your
    account.
  - This bucket will be used to store backups of your cassandra cluster
* IAM Role (Instance Profile)
  - Priam uses IAM Roles (Instance Profiles) to provide it's access to your AWS account resources.
  - This alleviates the need to put an Access ID or Secret Key on the actual EC2 instance
  - Once logged in to the AWS web console, go to the IAM page and select "Roles".  Create a role (any
    name is fine) and ensure it has the following policy:

        {
          "Statement": [
            {
              "Effect": "Allow",
              "Action": "ec2:*",
              "Resource": "*"
            },
            {
              "Effect": "Allow",
              "Action": "elasticloadbalancing:*",
              "Resource": "*"
            },
            {
              "Effect": "Allow",
              "Action": "cloudwatch:*",
              "Resource": "*"
            },
            {
              "Effect": "Allow",
              "Action": "autoscaling:*",
              "Resource": "*"
            },
            {
              "Effect": "Allow",
              "Action": "sdb:*",
              "Resource": "arn:aws:sdb:<your-region>:<your-account-number>:domain/InstanceIdentity"
            },
            {
              "Effect": "Allow",
              "Action": "s3:*",
              "Resource": ["arn:aws:s3:::cassandra-archive", "arn:aws:s3:::cassandra-archive/*"]
            }
          ]
        }

  - Any EC2 instance that you run priam on, should be associated with this role. This can be defined when you create
    your autoscaling group launch-config, like this:

        as-create-launch-config testing-launch-config --image-id ami-aecd60c7 --key mykey --group sg-xxxxxxx --instance-type m1.medium --region us-east-1 --iam-instance-profile my-instance-profile-name

* Auto Scaling Group
  - Every Priam/Cassandra node you run needs to be a part of an auto-scaling-group.
  - Then name of the auto-scaling-group is insignificant
  - Due to limitations with Amazon's auto-scaling policies (no guarantee that instances in the group will be balanced
    across all availability zones), it is recommended to create one auto-scaling-group per availability zone in each
    region you plan to operate your cluster.
* SimpleDB
  - Priam uses SimpleDB to coordinate token assignment throughout the cluster.
  - Nothing needs to be set up with SimpleDB ahead of time since Priam will create the resources (a Domain) it needs the first time it starts
  - You just need to ensure that your AWS account is signed up for SimpleDB


Starting Priam (which also starts Cassandra)
--------------------------------------------
You want to start priam with the same user that is ordinarily configured to start Cassandra itself

    sudo su - cassandra
    cd /usr/share/cassandra
    java -jar priam-x.y-SNAPSHOT.jar server priam.yaml

Cassandra Administration
--------------------------------------------
General Info
============
Example:
    
    curl -s "http://localhost:8080/v1/cassadmin/info" | python -mjson.tool
    {
        "data_center": "us-east",
        "generation_no": 1349814122,
        "gossip_active": true,
        "heap_memory_mb": "1454.5359420776367/7987.25",
        "load": "34.31 KB",
        "rack": "1a",
        "thrift_active": true,
        "token": "Token(bytes[1808575600])",
        "uptime": 84308
    }

Flush
============
Flushes all keyspaces and column families to disk.
Example:

    curl -s "http://localhost:8080/v1/cassadmin/flush" | python -mjson.tool
    {
        "result": "ok"
    }

Backup
--------------------------------------------
Status of Current Operations
============================
Example:

    curl -s "http://localhost:8080/v1/backup/status" | python -mjson.tool
    {
        "Backup": {
            "Status": "DONE",
            "Threads": 0
        },
        "Restore": {
            "Status": "DONE",
            "Threads": 0
        }
    }

List available Backups
======================
Available query parameters:

<table>
    <tr>
        <th>Query Param</th>
        <th>Format</th>
        <th>Required?</th>
        <th>Default</th>
    </tr>
    <tr>
        <td>daterange</td>
        <td>default, or "yyyyMMddHHmm,yyyyMMddHHmm"</td>
        <td>No</td>
        <td>One day in the past from right now</td>
    </tr>
    <tr>
        <td>filter</td>
        <td>SNAP, SST, CL or META</td>
        <td>No</td>
        <td>(no filtering)</td>
    </tr>
</table>

Example:

    curl -s "http://localhost:8080/v1/backup/list" | python -mjson.tool
    {
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/META/meta.json": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Data.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Digest.sha1": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Filter.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Index.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Statistics.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Data.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Digest.sha1": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Filter.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Index.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Statistics.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Data.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Digest.sha1": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Filter.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Index.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Statistics.db": "201210101656"
    }

Perform Full Snapshot
=====================
Example:

    curl -s "http://localhost:8080/v1/backup/do_snapshot" | python -mjson.tool
    {
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/META/meta.json": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Data.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Digest.sha1": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Filter.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Index.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-Versions-hd-1-Statistics.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Data.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Digest.sha1": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Filter.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Index.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_columnfamilies-hd-1-Statistics.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Data.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Digest.sha1": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Filter.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Index.db": "201210101656",
        "backup_ci_mbogner/us-east-1/ci_mbogner_sor_ugc_default/1808575600/201210101656/SNAP/system/system-schema_keyspaces-hd-1-Statistics.db": "201210101656"
    }