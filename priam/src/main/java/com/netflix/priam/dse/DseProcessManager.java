package com.netflix.priam.dse;

import java.util.List;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraProcessManager;
import com.netflix.priam.utils.Sleeper;

import static com.netflix.priam.dse.IDseConfiguration.NodeType;

public class DseProcessManager extends CassandraProcessManager
{
    private final IDseConfiguration dseConfig;

    @Inject
    public DseProcessManager(IConfiguration config, IDseConfiguration dseConfig, Sleeper sleeper)
    {
        super(config, sleeper);
        this.dseConfig = dseConfig;
    }

    protected List<String> getStartCommand()
    {
        List<String> cmd = super.getStartCommand();
        //looks like we always need to specify cassandra here
        cmd.add("cassandra");

        NodeType nodeType = dseConfig.getNodeType();
        if (nodeType == NodeType.ANALYTIC_HADOOP)
            cmd.add("-t");
        else if (nodeType == NodeType.ANALYTIC_SPARK)
            cmd.add("-k");
        else if (nodeType == NodeType.ANALYTIC_HADOOP_SPARK)
            cmd.add("-k -t");
        else if(nodeType == NodeType.SEARCH)
            cmd.add("-s");

        return cmd;
    }
}
