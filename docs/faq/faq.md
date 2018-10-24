# FAQ
## How do I enable authentication and/or authorization on already running Cassandra cluster
If you already have running cassandra cluster with Priam, you can follow the steps mentioned [here](enable_auth.md). 

## Increasing Cassandra cluster
There are 3 ways to increase the Cassandra cluster capacity: 
1. Instance Upgrade: With this you replace the instance types of the Cassandra cluster by more/less powerful instances, one by one. **Note**: This does not increase the number of coordinator nodes. 
2. [Doubling the cassandra cluster](expanding_cassandra.md): This will double the number of instances in your Cassandra cluster. This is usually preferred way to go (if possible) as this truly doubles the capacity of the Cassandra cluster by providing more co-ordinates nodes and gives more JVM's. 
3. [Add a new region](expanding_cassandra.md) to existing Cassandra cluster.   