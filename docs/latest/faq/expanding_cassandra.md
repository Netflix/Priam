# Expanding Cassandra Cluster
Priam currently only supports one token per instance paradigm. This allows for a Cassandra cluster to be only doubled in number of instances, to increase the capacity. 

The idea behind doubling the size the Cassandra cluster is we expect all the instances to take equal `load`. 

## Doubling the existing cluster

1. Ensure that Cassandra cluster is healthy and all the Cassandra instances are UP and serving traffic. 
1. Run a diagnostic check on this cluster by following these steps: 
    1. Take a backup of the token metadata for this cluster. 
    1. Ensure that you have a backup of this cluster in the remote file system and it is valid. Refer to [backups](../mgmt/backups.md) to see how to validate the backups. 
    1. Check for any host id collisions by calling `nodetool info` on all the Cassandra instances.
1. Execute a double ring command from any **ONE** priam instance. This will double the entries of tokens in the token data store. **Note**: Sometimes this call may take up to 5 minutes to execute. Do NOT run this command again. 
1. Ensure that token data store has double the tokens and take a backup of this `doubled` entries. 
1. Now add one instance to the ASG and wait for it to stream completely from the neighbors. Repeat this process till the size of the ASG has been doubled. 
1. Repeat above step for all the ASG's in the region. 
1. Once all the nodes have streamed and joined the ring, run the `nodetool cleanup` on old instances. This will delete the data from old instances which they do not own anymore.   

## Expanding to a new region for an existing cluster

1. Ensure that `priam.create.new.token` is `true` for the region where we want to exapnd. 
1. Create new security groups, IAM Roles, S3 buckets and ASG's for the cluster. 
1. Ensure that IAM Role has permission to access the S3 buckets. 
1. Ensure that Cassandra cluster is healthy and all the Cassandra instances are UP and serving traffic.
1. Add the required number of instances in the ASG to match other ASG from other region. 
1. Once they join the ring, disable the `priam.create.new.token` by setting this to `false`. 
1. Expand the keyspace definition to include this new region. Example: 
    ```sql
   cqlsh> DESC KEYSPACE user_keyspace; 
   CREATE KEYSPACE user_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east': '3'}  AND durable_writes = true;
   cqlsh> Alter KEYSPACE user_keyspace WITH replication = { 'class': 'NetworkTopologyStrategy',  'us-east': '3', 'us-east-2': '3'};
    ```
1. Run `nodetool rebuild` on all the instances. Recommendation is to run this on one ASG first, ensure it is finished and then other ASG. This is to avoid huge spike in cross-region streaming between existing Cassandra instances (serving live traffic) and these new instances. 
1. Run `nodetool repair` on the entire fleet before declaring newly added region available.  
 