package com.netflix.priam.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

/*
 * This task checks if the Cassandra process is running.
 */
@Singleton
public class CassandraMonitor extends Task{

	public static final String JOBNAME = "CASS_MONITOR_THREAD";
	public static final String CASSANDRA_PROCESS_NAME = "NFThinCassandraDaemon";
    private static final Logger logger = LoggerFactory.getLogger(CassandraMonitor.class);
    private static final AtomicBoolean isCassandraStarted = new AtomicBoolean(false);

    @Inject
    protected CassandraMonitor(IConfiguration config) {
		super(config);
	}

	@Override
	public void execute() throws Exception {

        try
        {
                        //This returns pid for the Cassandra process
        		Process p = Runtime.getRuntime().exec("pgrep -f " + CASSANDRA_PROCESS_NAME);
        		BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String line;
        		if ((line = input.readLine()) != null&& !isCassadraStarted())
        		{
        			logger.debug("Setting Cassandra server started flag to true");
        			//Setting cassandra flag to true
        			isCassandraStarted.set(true);
        		}
        }
        catch(Exception e)
        {
        		logger.warn("Exception thrown while checking if Cassandra is running or not ", e);
            logger.info("Setting Cassandra server started flag to false");
            //Setting Cassandra flag to false
            isCassandraStarted.set(false);
        }
		
	}

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public static Boolean isCassadraStarted()
    {
        return isCassandraStarted.get();
    }

}
