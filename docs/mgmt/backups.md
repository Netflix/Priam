# Backups
Priam supports both snapshot and incremental backups for Cassandra SSTable files and uses S3 to save these files.


## Backup file path
To identify backup files and to avoid collision, Priam organizes the files in S3 by including time, keyspace name, token, date, DC/region and cluster name. Here is the general format:

```text
<App Prefix>/<Region or DC>/<Cluster name>/<Token>/<Date & Time>/<File type>/<Keyspace>/<FileName>
```

Here are valid file types (refer _AbstractBackupPath.BackupFileType_):

```text
SNAP - Snapshot file
META - Meta file for all snapshot files 
SST - Incremental SSTable file
```
Here is a sample snapshot file in S3:

```text
test_backup/eu-west-1/cass_test/37809151880104273718152734159458104939/201202142346/SNAP/MyKeyspace/MyCF-g-265-Data.db
```


## Multipart upload

Priam leverages the multipart upload feature of S3.  This helps in uploading large files in parallel.  Each file is chunked into several parts and each part is uploaded in parallel. 

## Compression
Priam uses snappy compression to compress data files.  Each file is chunked and compressed on the fly. 

## Throttling
Throttling of uploads helps reduce disk IO and network IO as well. With Priam properties, you can set the Mb/s which are read from the disk. 

## SNS Notifications
Priam can send SNS Notifications, pre and post upload of any file to S3. This allows a cleaner integration with external service depending on backups which are about to be uploaded. 

## Async Uploads 
Priam allows user to configure if they wish to do parallel uploads to remote file system for snapshots and incrementals. Doing parallel uploads makes your backup faster but may use the bandwidth of the instance and thus starve Cassandra. Bandwidth used by backups can be controlled by `throttling` them. This should be carefully reviewed based on your instance capabilities. 

Priam by default does NOT allow async uploads, though our suggestion is to enable them on few instances and see the performance and then tune. In our internal testing, we have hardly found Cassandra to be starved by bandwidth usage (even during peak).  

## Configuration
1. **_priam.s3.bucket_**: This is the default S3 location where all backup files will be uploaded to. E.g. ```s3_bucket_region```. Default: ```cassandra-archive```
2. **_priam.upload.throttle_**: This allows backup/incremental's to be throttled so they don't end up taking all the bandwidth from Cassandra. Note: try to schedule full snapshot during off-peak time. Tweak this only if required else your snapshot would starve. Default: ```MAX_INT_VALUE```. 
3. **_priam.backup.notification.topic.arn_**: Amazon Resource Name for the SNS service where notifications need to be sent. This assumes that all the topics are created and the instance has permission to send SNS messages. Disabled if the value is null or empty. Default: ```None```. 
4. **_priam.backup.chunksizemb_**: Size of the file in MB above which Priam will try to use multi-part uploads. Note that chunk size of the file being uploaded is as defined by user only if total parts to be uploaded (based on file size) will not exceed 10000 (max limit from AWS). If file is bigger than 10000 * configured chunk size, Priam will adjust the chunk size to ensure it can upload the file. Default: _10 MB_ 
5. **_priam.backup.queue.size_**: Queue size to be used for backup uploads. Note that once queue is full, we would wait for `priam.upload.timeout` to add any new item before declining the request and throwing exception. Default: 100,000. 
6. **_priam.backup.threads_**: The number of backup threads to be used to upload files to S3. Default: ```2```
7. **_priam.upload.timeout_**: Uploads are scheduled in `priam.backup.queue.size`. If queue is full then we wait for this time for the queue to have an entry available for queueing the current task. Default: 2 minutes. 

# Snapshot or complete backup
Priam leverages Cassandra's snapshot feature to have an eventually consistent backup.  Cassandra's snapshot feature flushes data to disk and hard links all SSTable files into a snapshot directory. These files are then picked up by Priam and uploaded to S3.  Although snapshotting across the cluster is not guaranteed to produce a consistent backup of the cluster, consistency is resumed upon restore by Cassandra and running repairs. 
Priam uses Quartz to schedule all tasks.  Snapshot backups are run as a recurring task.  They can also be triggered via REST API (refer API section), which is useful during upgrades or maintenance operations.  
Snapshots are run on a CRON for the entire cluster, at a specific time, ideally during non-peak hours. Priam's _backup.hour_ or _backup.cron_ property allows you to set daily snapshot time (refer properties). The snapshot backup orchestrates invoking of Cassandra **_snapshot_** JMX command, uploading files to S3 and cleaning up the directory.  

## meta.json

All SSTable files are uploaded individually to S3 with built-in retries on failure.  Upon completion, the meta file (_meta.json_) is uploaded which contains a reference to all files that belong to the snapshot.   This is then used for validation during restore.   


## Backup Status Management

Priam by default stores the status of all the snapshot. This is useful to determine if the snapshot at a given Instant was successful. It has support to store status of multiple snapshots for a given date. 

The default binding is to store these details to a local file configured via `priam.backup.status.location`. 


## Validation of Snapshot
Priam can download the ```meta.json``` for a given snapshot, parse it and then validate the backup by ensuring that all the files listed in file are available on remote file system. This can be used to verify that no file was missed during snapshot. 

### Snapshot Configuration

1. **_priam.backup.schedule.type_**: This allows you to choose between 2 supported schedule types (CRON or HOUR) for backup. The default value is ```HOUR```. 
* ```CRON```: This allows backup to run on CRON and it expects a valid value of **_priam.backup.cron_** to be there. If not, priam will not start and fail fast. 
* **Deprecated**: ```HOUR```: This allows backup to run on a daily basis at a given hour. It expects a valid value of **_priam.backup.hour_** to be there. If not, priam will not start and fail fast. 
2. **Deprecated**: **_priam.backup.hour_**: This allows backup to run on a daily basis at a given hour. If the value is ```-1```, it means snapshot backups and incremental are disabled. Default value: ```12```
3. **_priam.backup.cron_**: This allows backups to be run on CRON. Example: you want to run a backup once a week. Value needs to be a valid CRON expression. To disable backup, use the value of ```-1```. The default value is to backup at ```12```. 
5. **_priam.snapshot.cf.include_**: Column Family(ies), comma delimited, to include for snapshot. If no override exists, all keyspaces/cf's will be backed up to remote file system. The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Snapshot exclude list is applied first to exclude CF/keyspace and then snapshot include list is applied to include the CF's/keyspaces. Default: ```None```
6. **_priam.snapshot.cf.exclude_**: Column Family(ies), comma delimited, to ignore while doing snapshot. The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Snapshot exclude list is applied first to exclude CF/keyspace and then snapshot include list is applied to include the CF's/keyspaces. Default: ```None```
7. **_priam.backup.status.location_**: The absolute path to store the backup status on disk. Default: ```<data file location>/backup.status```. 
8. **_priam.async.snapshot_**: This decides if snapshot files should be uploaded to the remote file system in async fashion. This will allow snapshot to use the `priam.backup.threads` to do parallel uploads. Default: ```false```. 

## API

### Execute Snapshot
> ```http://localhost:8080/Priam/REST/v1/backup/do_snapshot```

This executes a one time snapshot on this instance and starts uploading the files to remote file system. 

**Output:** 
```json
{"ok"}
 ```

### Backup Status
> ```http://localhost:8080/Priam/REST/v1/backup/status```

Get the status of the last known backup. If priam is restarted, it will always show `DONE`. 

**Output:** 
```json
{"SnapshotStatus":"[DONE|ERROR|RUNNING|NOT_APPLICABLE]"}
```

### Backup Status for a date
> ```http://localhost:8080/Priam/REST/v1/backup/status/{date}```

Determines the status of a snapshot for a date.  If there was at least one successful snapshot for the date, snapshot for the date is considered completed.

**Parameters:** 
* {date}: Required: Date of the snapshot to check in the format of `yyyyMMdd`. 

**Output:**

For success: 
```json
{"Snapshotstatus":true,
"token":"<token_value>",
"starttime":"[yyyyMMddHHmm]",
"completetime":"[yyyyMMddHHmm]"}
```

For failure: 
```json
{"Snapshotstatus":true,
"token":"<token_value>"}
```

### List all snapshots
> ```http://localhost:8080/Priam/REST/v1/backup/status/{date}/snapshots```

List all the snapshot executed for a date. 

**Parameters:** 
* {date}: Required: Date of the snapshot to check in the format of `yyyyMMdd`. 

**Output:**
```json
{"Snapshots":["yyyyMMddHHmm","yyyyMMddHHmm"]}
```

### Validate snapshot
> ```http://localhost:8080/Priam/REST/v1/backup/validate/snapshot/{daterange}```

Determines the validity of the backup by 

1. Downloading meta.json file 
1. Listing of the backup directory  
1. Find the missing or extra files in backup location.

This by default takes the latest snapshot of the application. One can provide exact hour and min to check specific backup. **Note**: This is an expensive call (money and time) as it calls list on the remote file system, thus should be used with caution.

**Parameters:**

*`daterange`: Optional: This is a comma separated start time and end time for the snapshot in the format of `yyyyMMddHHmm` or `yyyyMMdd`. 
  If no value is provided or a value of `default` is provided then it takes start time as (current time - 1 day) and end time as current time.
  
**Output:**
```json
{  
  "inputStartDate":"[yyyyMMddHHmm]",
  "inputEndDate":"[yyyyMMddHHmm]",
  "snapshotAvailable":true,
  "valid":true,
  "backupFileListAvailable":true,
  "metaFileFound":true,
  "selectedDate":"[yyyyMMdd]",
  "snapshotTime":"[yyyyMMddHHmm]",
  "filesInMetaOnly":[  

  ],
  "filesInS3Only":[  
    "file1"
  ],
  "filesMatched":[  
    "file1",
    "file2"
  ]
}
``` 

### List the backup files  
> ```http://localhost:8080/Priam/REST/v1/backup/list/{daterange}```

List all the files in the remote file system for the given daterange. **Note**: This is an expensive call (money and time) as it calls list on the remote file system, thus should be used with caution.

**Parameters:**

*`daterange`: Optional: This is a comma separated start time and end time for the snapshot in the format of `yyyyMMddHHmm` or `yyyyMMdd`. 
  If no value is provided or a value of `default` is provided then it takes start time as (current time - 1 day) and end time as current time.
  
**Output:**
```json
{
  "files": [
    {
      "bucket": "<remote_file_system>",
      "filename": "file",
      "app": "<app_name>",
      "region": "<region>",
      "token": "<token>",
      "ts": "[yyyyMMddHHmm]",
      "instance_id": "<instance_id>",
      "uploaded_ts": "[yyyyMMddHHmm]"
    }
],
  "num_files": <no_of_files>
}
```
# Incremental backup

When incremental backups are enabled in Cassandra, hard links are created for all new SSTables created in the incremental backup directory. Since SSTables are immutable files they can be safely copied to an external source. Priam scans this directory frequently for incremental SSTable files and uploads to S3. 

## Incremental Configuration
1. **_priam.backup.incremental.enable_**: This allows the incremental backups to be uploaded to S3. By default every 10 seconds, Priam will try to upload any new files flushed/compacted by Cassandra to disk. Default: ```true```
2. **_priam.incremental.cf.include_**: Column Family(ies), comma delimited, to include for incremental backups. If no override exists, all keyspaces/cf's will be backed up to remote file system. The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Incremental exclude list is applied first to exclude CF/keyspace and then incremental include list is applied to include the CF's/keyspaces. Default: ```None```
3. **_priam.incremental.cf.exclude_**: Column Family(ies), comma delimited, to ignore while doing incremental backup. The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Incremental exclude list is applied first to exclude CF/keyspace and then incremental include list is applied to include the CF's/keyspaces. Default: ```None```
4. **_priam.async.incremental_**: Allow upload of incremental's in parallel using multiple threads. Note that this will take more bandwidth of your cluster and thus should be used with caution. It may be required when you do repair your instance primary token range using subrange repair creating a lot of small files during the process. Default: ```false```

## API
> ```http://localhost:8080/Priam/REST/v1/backup/incremental_backup```

Enable the incremental backup on this instance. **Note**: This call does not change the value of the property `priam.backup.incremental.enable`

**Output:** 
```json
{"ok"}
```

# Commit log Configuration
1. **_priam.clbackup.enabled_**: This allows the backup of the commit logs from Cassandra to the backup location. The default value to check for new commit log is 1 min. Default: ```false```

# Encryption of Backup/Restore
Backups can be encrypted by Priam. Priam can also restore from the encrypted backups for disaster recovery. Note that encryption/decryption is generally CPU and time-intensive process. While uploading the file, it would be first compressed and then encrypted. The reverse of that happens when downloading the file i.e. decrypt and then decompress. 

Priam uses PGP as the encryption / decryption cryptography algorithm. Other algorithm can be implemented via interface IFileCryptography.

*Note: Pgp uses a passphrase to encrypt your private key on your node. See (http://www.pgpi.org/doc/pgpintro/), section “what is a passphrase” for details.
## Configuration
1. **_priam.encrypted.backup.enabled_**: Allow encryption while uploading of the snapshot, incremental and commit logs. Default: ```false```
3. **_priam.pgp.password.phrase_**: The passphrase used by the cryptography algorithm to encrypt/decrypt. By default, it is expected that this value will be encrypted using open encrypt. Default: ```None```.  
4. **_priam.private.key.location_**: The location on disk of the private key used by the cryptography algorithm. 
5. **_priam.pgp.pubkey.file.location_**: The location on disk of the public key used by the cryptography algorithm. 

