package com.netflix.priam.dse;

import java.util.Map;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraProcessManager;
import com.netflix.priam.dse.IDseConfiguration.NodeType;
import com.netflix.priam.utils.Sleeper;

public class DseProcessManager extends CassandraProcessManager
{
    private final IDseConfiguration dseConfig;

    @Inject
    public DseProcessManager(IConfiguration config, IDseConfiguration dseConfig, Sleeper sleeper)
    {
        super(config, sleeper);
        this.dseConfig = dseConfig;
    }

    protected void setEnv(Map<String, String> env) {   
        super.setEnv(env);

        NodeType nodeType = dseConfig.getNodeType();
        if (nodeType == NodeType.ANALYTIC_HADOOP)
            env.put("CLUSTER_TYPE", "-t");
        else if (nodeType == NodeType.ANALYTIC_SPARK)
            env.put("CLUSTER_TYPE", "-k");
        else if (nodeType == NodeType.ANALYTIC_HADOOP_SPARK)
            env.put("CLUSTER_TYPE", "-k -t");
        else if(nodeType == NodeType.SEARCH)
            env.put("CLUSTER_TYPE", "-s");
    }
  
}
