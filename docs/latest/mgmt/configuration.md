# Configuration

Priam exposes multiple configuration via `IConfiguration.java`. You can choose to override the defaults configuration used by Priam by specifying properties.  The defaults provided on this page are implemented by `PriamConfiguration.java` class. 

Priam has several ConfigSource bindings. The ordering of these ConfigSources determines how Priam will resolve the properties for a given instance. These sources also allow other config sources to be attached. 

Priam by default ships with hookup for [`SimpleDB`](https://aws.amazon.com/simpledb/) as the ConfigSource. 

`PriamConfiguration` looks for properties specified in the **PriamProperties** SimpleDB domain.  To create a property, create an item with the following attributes (the item name is not important, but it may be beneficial to set it to _cluster name + property name_):
* **appId** : Your cluster name
* **property**: Name of the property
* **value**: Property value

_Cluster name_ is inferred from your ASG name( _my_cluster_).  Multi zone clusters should have ASG suffixed with a zone identifier '-{zone}' ( _my_cluster-useast1a_), i.e., {clustername}-{zone}.

Example: If you want to change the jmx port to **7200** for your cluster 'my_cluster', create the item:
* **Item name** : _my_clusterpriam.jmx.port_
* **appId** : _my_cluster_
* **property**: _priam.jmx.port_
* **value**: _7200_  


## API

Priam exposes all the configuration items used to configure various components in Priam via REST API. 

### Get all configurations
> `http://localhost:8080/Priam/REST/v1/config/structured/group`

This returns all the configuration as resolved by Priam in a JSON blob. 

### Get a configuration
> `http://localhost:8080/Priam/REST/v1/config/structured/group/{name}`

This will return the configuration (as JSON) as resolved by Priam. **Note**: This requires the caller to know the name of the java functions, and is thus not recommended. 

Example for _success_: 
```text
curl http://localhost:8080/Priam/REST/v1/config/structured/group/dynamicSnitchEnabled
{"dynamicSnitchEnabled":true}
```

In case of _failure_: 
```json
{"message":"No such structured config: [queueSize]"}
```

## Configuration dump to local file
Priam also dumps these `resolved` configurations (from multiple ConfigSource) into a local file at regular intervals (default: 1 min) so that any other tool using Priam can consume these configurations. 
**Note**: Priam by default changes the permission of this file so non Priam users cannot read it, as configurations may contain sensitive information. 

### Configuration
1. **_priam.configMerge.cron_**: Cron expression to get the `resolved` configurations dumped to local file. Default: `0 * * * * ? *` (1 minute). 
2. **_priam.configMerge.dir_**: Directory where to dump the `resolved` configurations. Default: `/tmp/priam_configuration`.
