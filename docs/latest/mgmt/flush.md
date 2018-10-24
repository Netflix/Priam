# Flush 
Flush the data in Memtables on 1:* keyspaces from memory to disk
## Configuration
1. **_priam.flush.keyspaces_**: To flush specific keyspaces, set property and provide a comma-delimited list of values (e.g. perftest,dse_system). If no override exists, all keyspaces (EXCLUDING system) will be flushed. Default: ```None```
2. **_priam.flush.schedule.type_**: This allows you to choose between 2 supported schedule types (CRON or HOUR) for the flush. The default value is ```HOUR```. 
* ```CRON```: This allows flush to run on CRON and it expects either a valid value of **_priam.flush.cron_** or no override(null) to be there. If not, priam will not start and fail fast. 
* **Deprecated** ```HOUR```: This allows flush to run either on a daily basis at a given hour or hourly at a certain minute. It expects either a valid value of **_priam.flush.interval_** or no override(null) to be there. If not, priam will not start and fail fast. 
3. **_priam.flush.cron_**: This allows flush to be run on CRON. Example: you want to run flush every 15 minutes. Value needs to be a valid CRON expression. To disable flush, remove any overrides i.e. return "-1" for this value. The default value is ```-1```. 
4. **Deprecated**: **_priam.flush.interval_**: Set this property to specify the interval between flushes. The value is a name=value where _name_ is an enum of _hour_ or _daily_. Default: ```None``` 
* _hour_ means the flush will run at every hour.  The value is an integer representing the minute.  E.g. ```hour=0``` will run on the hour, every hour.

* _daily_ means the flush will run once daily at the specified time.  The value is an integer representing the hour. E.g. ```daily=10``` will run at 10:00 a.m. daily.

## API
> ```http://localhost:8080/Priam/REST/v1/cassadmin/flush```

**Output:** 
* For success, the payload is ```{"Flushed":true}```
* For failure, the payload will be ```{"status":"ERROR", "desc":"error_reason"}```