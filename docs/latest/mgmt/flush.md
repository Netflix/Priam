# Flush 
Flush the data in Memtables on 1:* keyspaces from memory to disk
## Configuration
1. **_priam.flush.keyspaces_**: To flush specific keyspaces, set property and provide a comma-delimited list of values (e.g. perftest,dse_system). If no override exists, all keyspaces (EXCLUDING system) will be flushed. Default: ```None```
1. **_priam.flush.cron_**: This allows flush to be run on CRON. Example: you want to run flush every 15 minutes. Value needs to be a valid CRON expression. To disable flush, remove any overrides i.e. return "-1" for this value. The default value is ```-1```. 


## API
> ```http://localhost:8080/Priam/REST/v1/cassadmin/flush```

**Output:** 
* For success, the payload is ```{"Flushed":true}```
* For failure, the payload will be ```{"status":"ERROR", "desc":"error_reason"}```