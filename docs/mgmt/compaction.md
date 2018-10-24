
# Compaction 
Compact the data in SSTables on 1:* keyspaces/CF. 
## Configuration
1. **_priam.compaction.cf.include_**: Column Family(ies), comma delimited, to start compactions (user-initiated or on CRON). If no override exists, all keyspaces (EXCLUDING system) will be compacted. The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Compaction exclude list is applied first to exclude CF/keyspace and then compaction include list is applied to include the CF's/keyspaces. Default: ```None```
2.  **_priam.compaction.cf.exclude_**: Column Family(ies), comma delimited, to ignore while doing compactions (user-initiated or on CRON). The expected format is keyspace.cfname. CF name allows special character **"_*_"** to denote all the columnfamilies in a given keyspace. e.g. keyspace1.* denotes all the CFs in keyspace1. Compaction exclude list is applied first to exclude CF/keyspace and then compaction include list is applied to include the CF's/keyspaces. Default: ```None```
3. **_priam.compaction.cron_**: This allows compactions to be run on CRON. Example: you want to run compaction every 1 hour. Value needs to be a valid CRON expression. To disable compaction, remove any overrides i.e. return -1 for this value. The default value is ```-1```. 


## API
> ```http://localhost:8080/Priam/REST/v1/cassadmin/compcat```

**Output:** 
* For success, the payload is ```{"Compcated":true}```
* For failure, the payload will be ```{"status":"ERROR", "desc":"error_reason"}```