package com.netflix.priam.utils;

import java.io.IOException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

@Singleton
public class TuneCassandra extends Task
{
    public static final String JOBNAME = "Tune-Cassandra";
    private final CassandraTuner tuner;

    @Inject
    public TuneCassandra(IConfiguration config, CassandraTuner tuner)
    {
        super(config);
        this.tuner = tuner;
    }

    public void execute() throws IOException
    {
        tuner.updateYaml(config.getYamlLocation(), null, config.getSeedProviderName());
    }

    @Override
    public String getName()
    {
        return "Tune-Cassandra";
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }
}
