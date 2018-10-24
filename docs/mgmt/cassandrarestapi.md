
# Common prefix

It will depend on how you deploy your application.

In our case, we use it with following prefix: "http://127.0.0.1:8080/Priam/REST"

E.g. to invoke get_token, which is under "/v1/cassconfig", the call is "http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_token"

# Cassandra Config related

|**API** |**Description**|**Query params**|
|/v1/cassconfig/get_seeds |Gets a list of seeds. One per zone/Rack |  |
|/v1/cassconfig/get_token |Get token for the node | |
|/v1/cassconfig/is_replace_token |Returns _true_ if this node is replace another node with the same token | |
|/v1/cassconfig/double_ring |Double the ring | |

# Cassandra admin

|**API** |**Description**|**Query params**|
|/v1/cassadmin/start |Starts Cassandra process||
|/v1/cassadmin/stop |Stop Cassandra process |  |

# Nodetool commands

|**API** |**Description**|**Query params**|
|/v1/cassadmin/info | Get info (nodetool info) in json format |  |
|/v1/cassadmin/partitioner | Get partitioner name |  |
|/v1/cassadmin/ring/KEYSPACE | Get ring (nodetool ring) in json format. | Provide a Keyspace parameter |
|/v1/cassadmin/flush |flushes all keyspaces |   |
|/v1/cassadmin/compact | Run compaction |  |
|/v1/cassadmin/cleanup | Run cleanup |  |
|/v1/cassadmin/repair | Run repair (nodetool repair) |  |
|/v1/cassadmin/refresh?keyspaces=<KEYSPACES> | |KEYSPACES: Comma seperated list of keyspaces |
    |/v1/cassadmin/version | Show release version | |
    |/v1/cassadmin/tpstats | Show Thread pool stats | |
    |/v1/cassadmin/compactionstats | Show Compaction stats | |
    |/v1/cassadmin/disablegossip | Disable gossip | |
    |/v1/cassadmin/enablegossip | Enable gossip | |
    |/v1/cassadmin/disablethrift | Disable Thrift | |
    |/v1/cassadmin/enablethrift | Enable Thrift | |
    |/v1/cassadmin/statusthrift | Show Thrift Status | |
    |/v1/cassadmin/gossipinfo | Show Gossip Info | |
    |/v1/cassadmin/netstats?host=<HOST> | Show Net Stats |_HOST_ (optional)|
        |/v1/cassadmin/move?token=<NEWTOKEN> | Move | Provide New Token |
            |/v1/cassadmin/scrub?keyspaces=<KEYSPACES>&cfnames=<CFNAMES> | Run Scrub |KEYSPACES,CFNAMES(optional) |
                |/v1/cassadmin/cfhistograms?keyspaces=<KEYSPACES>&cfnames=<CFNAMES> | Run CF Histogram |KEYSPACES,CFNAMES |