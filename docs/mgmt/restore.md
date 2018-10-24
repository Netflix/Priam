# Restore
Data from a snapshot can be restored to the same cluster or a different cluster.  To restore a cluster, the user specifies a start and end time for which the backup data is available.  Priam executes the following sequence of steps for restoring:

* Search for the latest snapshot within the time range 
* Download meta file for the snapshot
* Download snapshot files specified in the meta file
* Download all available incremental files up to the end time
* Execute a post restore hook if available, and wait for completion of the hook execution
* Start the cluster and join the ring
The _LocationInfo_ files are ignored and not backed up.  This forces Cassandra to rediscover other nodes in the cluster upon restore and refresh the ring nodes. 

**Important**: Restores should only be performed on the same size cluster. 

## Async Downloads

During restore, the instance is **_not in service_** and thus Priam tries its best to utilize available bandwidth to download the SSTables from remote file system. We use `8` threads by default to download the files. 
 
## Configuration
1. **_priam.restore.prefix_**: This is the location from where the backup will be restored. This has to be the fully qualified location where cluster backup is stored (till cluster name). E.g. ```bucket_name/test_backup/test/us-east-1/cass_appname```. Default: ```None```
2. **_priam.restore.snapshot_**: This is the start dateTime and end dateTime from which cluster will be restored. Priam will start finding the latest full snapshot from the end dateTime and will keep going till start dateTime. After the full snapshot is found, Priam downloads all the incremental's. E.g. ```2017040500,201704092359```. Default: ```None```
3. **_priam.restore.threads_**: The number of threads to use for restore. Default: ```8```
4. **_priam.restore.cf.include_**: Column Family(ies), comma delimited, to include for restore. If no override exists, all keyspaces/cf's will be restored. The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Restore exclude list is applied first to exclude CF/keyspace and then restore include list is applied to include the CF's/keyspaces. Default: ```None```
5. **_priam.restore.cf.exclude_**: Column Family(ies), comma delimited, to ignore while doing restore. The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Restore exclude list is applied first to exclude CF/keyspace and then restore include list is applied to include the CF's/keyspaces. Default: ```None```
7. **_priam.clrestore.max_**: Max number of commit logs to restore. This will by default take last N number of commit logs to restore if commit log backup is enabled. Default: ```10```. 
8. **_priam.postrestorehook.enabled_**: indicates if postrestorehook is enabled.
9. **_priam.postrestorehook_**: contains the command with arguments to be executed as part of postrestorehook. Priam would wait for completion of this hook before proceeding to starting C*.
10. **_priam.postrestorehook.heartbeat.filename_**: heartbeat file that postrestorehook emits. Priam keeps a tab on this file to make sure postrestorehook is making progress. Otherwise, a new process of postrestorehook would be spawned (upon killing existing process if still exists)
11. **_priam.postrestorehook.done.filename_**:'done' file that postrestorehook creates upon completion of execution.
12. **_priam.postrestorehook.timeout.in.days_**:maximum time that Priam should wait before killing the postrestorehook process (if not already complete)
13. **_priam.download.queue.size_**: Queue size to be used for restore downloads. Note that once queue is full, we would wait for `priam.download.timeout` to add any new item before declining the request and throwing exception. Default: 100,000. 
14. **_priam.download.timeout_**: Downloads are scheduled in `priam.download.queue.size`. If queue is full then we wait for this time for the queue to have an entry available for queueing the current task. Default: 10 minutes. 

# Support for Encrypted Backups

Priam restore will handle decryption of the encrypted files. Any file downloaded from remote file system will be first decrypted and then decompressed to ephemeral disk. 
As part of the restore, Priam will decrypt the ciphertext to plaintext if property (_priam.encrypted.restore.enabled_) is set to true.

Priam uses PGP as the encryption / decryption cryptography algorithm. Other algorithm can be implemented via interface IFileCryptography.


*Note: Pgp uses a passphrase to encrypt your private key on your node. See (http://www.pgpi.org/doc/pgpintro/), section “what is a passphrase” for details.

Priam restore from AWS bucket using “role assumption”:
The use case is you are trying to restore objects created by different AWS accounts. See AWS documentation for “cross account access using roles” for detail information.

Currently, Google cloud and secondary/cross AWS accounts are supported for restoring the encrypted backups. 

## Configuration
1. **_priam.encrypted.restore.enabled_**: Enable the restore where the backups are encrypted using PGP. 
3. **_priam.pgp.password.phrase_**: The passphrase used by the cryptography algorithm to encrypt/decrypt. By default, it is expected that this value will be encrypted using open encrypt. Default: ```None```.  
4. **_priam.private.key.location_**: The location on disk of the private key used by the cryptography algorithm. 
5. **_priam.pgp.pubkey.file.location_**: The location on disk of the public key used by the cryptography algorithm. 
3. **_priam.restore.source.type_**: The type of source for the restore.  Valid values: AWSCROSSACCT or GOOGLE. Default: ```None```
1. **_priam.gcs.service.acct.id_**: Google Cloud Storage service account id. This value is supposed to be encrypted using open encrypt. Default: ```None```
2. **_priam.gcs.service.acct.private.key_**: The absolute path on disk for the Google Cloud Storage PFX file (i.e. the combined format of the private key and certificate). This value is supposed to be encrypted using open encrypt. Default: ```None```
4. **_priam.roleassumption.arn_**: Amazon Resource Name to assume while restoring the backups from an AWS account which requires cross-account assumption. Default: ```None```

# API

#### Restore Status
> ```http://localhost:8080/Priam/REST/v1/restore/status```

  This gives the status of the current or last restore. Note that the restore status is not persisted on ephemeral disk and thus this status may be lost at restart of Priam. 
  
  **Output**: 
  ```json
    {
      "startDateRange": "[yyyyMMddHHmm]",
      "endDateRange": "[yyyyMMddHHmm]",
      "executionStartTime": "[yyyyMMddHHmm]",
      "executionEndTime": "[yyyyMMddHHmm]",
      "snapshotMetaFile": "<remote_file_system_location_of_meta.json_used_for_restore>",
      "status": "[STARTED|FINISHED|FAILED]"
    }
   ```
#### Manual Restore
> ```http://localhost:8080/Priam/REST/v1/restore/{daterange}```
  
  This starts a user-triggered restore for a given daterange. **Note**: It expects that other restore configurations are already configured. It requires a minimum of `priam.restore.prefix` to be already configured. 
  
  **Parameters**: 
  1. `daterange`: Optional: This is a comma separated start time and end time for the restore in the format of `yyyyMMddHHmm` or `yyyyMMdd`. 
  If no value is provided or a value of `default` is provided then it takes start time as (current time - 1 day) and end time as current time.
  
  **Output**: 
  ```json
  {"ok"}
  ```