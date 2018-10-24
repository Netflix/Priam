# Enabling Authentication and Authorization on Cassandra

By default, Priam does **NOT** enable authentication or authorization to Cassandra cluster. Any running Cassandra cluster can be converted to authenticated and/or authorized cluster. 

1. Make sure following parameters are either commented or do not exist in _cassandra.yaml_ before enabling authentication / authorization on Cassandra, if not done will generate huge schema differences. 
```text
auth_replication_strategy
auth_replication_options
replication_factor
```
2. Set Cassandra in transitional mode by setting the following properties:
```properties
priam.authenticator = com.datastax.bdp.cassandra.auth.TransitionalAuthenticator
priam.authorizer = com.datastax.bdp.cassandra.auth.TransitionalAuthorizer
```
This is important as it will allow existing clients to connect to Cassandra while we make all the changes. 

3. Re-start Priam and C* on a node and ensure that the system_auth KS is created by logging using the default cassandra user/password. 
    
    ```sql
    cqlsh -u cassandra -p cassandra
    cqlsh> use system_auth ;
    cqlsh:system_auth> desc TABLEs
    credentials  permissions  users
    cqlsh:system_auth> select * from system_auth.users ;
    
     name      | super
    -----------+-------
     cassandra |  True
    ```


4. Alter the system_auth KS to all the DC's your cluster is in. For _example_: 
```sql
Alter KEYSPACE system_auth WITH replication = { 'class': 'NetworkTopologyStrategy',  'us-east': '3', 'us-east-2': '3'};
```

5.  Create the users and alter the default password for user "_cassandra_".
```sql
Alter user cassandra with password ‘XYZ’ 
CREATE USER appuser1 WITH PASSWORD 'password';
```

6. Repair the keyspace to ensure data is propogated to all the instances. 
```
nodetool repair system_auth
```

7. Change the authenticator and authorizer property in Priam to following values: 
```properties
priam.auto.bootstrap = true
priam.authenticator = org.apache.cassandra.auth.PasswordAuthenticator
priam.authorizer = org.apache.cassandra.auth.CassandraAuthorizer
```

8. Perform a rolling restart of Priam and Cassandra on all the instances. 