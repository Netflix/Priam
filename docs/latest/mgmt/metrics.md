# Metrics

Priam uses [spectator](https://github.com/Netflix/spectator) to collect Metrics. The default binding is no operation. 

|Name|Type|Purpose|
|`priam.upload.rate`|DistributionSummary|Rate at which Priam is uploading the files to remote file system (during backup).|
|`priam.upload.valid`|Counter|No of successful uploads to the remote file system.|
|`priam.upload.invalid`|Counter|No. of uploads to the remote file system which failed after exhausting all the retries.|
|`priam.upload.queue.size`|Counter|Size of the upload queue where items are waiting to be uploaded.|
|`priam.download.rate`|DistributionSummary|Rate at which Priam is downloading the files from remote file system (during restore).|
|`priam.download.valid`|Counter|No of successful downloads from the remote file system.|
|`priam.download.invalid`|Counter|No. of downloads from the remote file system which failed after exhausting all the retries.|
|`priam.download.queue.size`|Counter|Size of the download queue where items are waiting to be downloaded.|
|`priam.sns.notification.success`|Counter|Total number of sns notifications sent which are successful|
|`priam.sns.notification.failure`|Counter|Total number of sns notifications which failed after exhausting all the retries (may be configuration error)|
|`priam.forgotten.files`|Counter|No. of files that are considered forgotten by Cassandra, as detected by Priam during snapshot. **Note**: This is exclusive feature of 3.x (C* 2.x) branch|
