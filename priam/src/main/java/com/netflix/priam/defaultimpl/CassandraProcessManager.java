/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.defaultimpl;

import com.google.common.collect.Lists;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.JMXNodeTool;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.merics.CassMonitorMetrics;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraProcessManager implements ICassandraProcess {
    private static final Logger logger = LoggerFactory.getLogger(CassandraProcessManager.class);
    private static final String SUDO_STRING = "/usr/bin/sudo";
    private static final int SCRIPT_EXECUTE_WAIT_TIME_MS = 5000;
    protected final IConfiguration config;
    private final InstanceState instanceState;
    private final CassMonitorMetrics cassMonitorMetrics;

    @Inject
    public CassandraProcessManager(
            IConfiguration config,
            InstanceState instanceState,
            CassMonitorMetrics cassMonitorMetrics) {
        this.config = config;
        this.instanceState = instanceState;
        this.cassMonitorMetrics = cassMonitorMetrics;
    }

    protected void setEnv(Map<String, String> env) {
        // If we can tune a jvm.options file instead of setting these
        // environment variables we prefer to set heap sizes that way
        if (!config.supportsTuningJVMOptionsFile()) {
            env.put("HEAP_NEWSIZE", config.getHeapNewSize());
            env.put("MAX_HEAP_SIZE", config.getHeapSize());
        }

        env.put("DATA_DIR", config.getDataFileLocation());
        env.put("COMMIT_LOG_DIR", config.getCommitLogLocation());
        env.put("LOCAL_BACKUP_DIR", config.getBackupLocation());
        env.put("CACHE_DIR", config.getCacheLocation());
        env.put("JMX_PORT", "" + config.getJmxPort());
        env.put("LOCAL_JMX", config.enableRemoteJMX() ? "no" : "yes");
        env.put("MAX_DIRECT_MEMORY", config.getMaxDirectMemory());
        env.put("CASS_LOGS_DIR", config.getLogDirLocation());
        env.put("CASSANDRA_LOG_DIR", config.getLogDirLocation());
    }

    public void start(boolean join_ring) throws IOException {
        logger.info("Starting cassandra server ....Join ring={}", join_ring);
        instanceState.markLastAttemptedStartTime();
        instanceState.setShouldCassandraBeAlive(true);

        List<String> command = Lists.newArrayList();

        if (config.useSudo()) {
            logger.info("Configured to use sudo to start C*");
            if (!"root".equals(System.getProperty("user.name"))) {
                command.add(SUDO_STRING);
                command.add("-n");
                command.add("-E");
            }
        }
        command.addAll(getStartCommand());

        ProcessBuilder startCass = new ProcessBuilder(command);

        Map<String, String> env = startCass.environment();
        setEnv(env);
        env.put("cassandra.join_ring", join_ring ? "true" : "false");

        startCass.directory(new File("/"));
        startCass.redirectErrorStream(true);
        logger.info("Start cmd: {}", startCass.command());
        logger.info("Start env: {}", startCass.environment());

        Process starter = startCass.start();

        logger.info("Starting cassandra server ....");
        try {
            int code = starter.waitFor();
            if (code == 0) {
                logger.info("Cassandra server has been started");
                instanceState.setCassandraProcessAlive(true);
                this.cassMonitorMetrics.incCassStart();
            } else logger.error("Unable to start cassandra server. Error code: {}", code);

            logProcessOutput(starter);
        } catch (Exception e) {
            logger.warn("Starting Cassandra has an error", e);
        }
    }

    protected List<String> getStartCommand() {
        List<String> startCmd = new LinkedList<>();
        for (String param : config.getCassStartupScript().split(" ")) {
            if (StringUtils.isNotBlank(param)) startCmd.add(param);
        }
        return startCmd;
    }

    void logProcessOutput(Process p) {
        try {
            final String stdOut = readProcessStream(p.getInputStream());
            final String stdErr = readProcessStream(p.getErrorStream());
            logger.info("std_out: {}", stdOut);
            logger.info("std_err: {}", stdErr);
        } catch (IOException ioe) {
            logger.warn("Failed to read the std out/err streams", ioe);
        }
    }

    String readProcessStream(InputStream inputStream) throws IOException {
        final byte[] buffer = new byte[512];
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.length);
        int cnt;
        while ((cnt = inputStream.read(buffer)) != -1) baos.write(buffer, 0, cnt);
        return baos.toString();
    }

    public void stop(boolean force) throws IOException {
        logger.info("Stopping cassandra server ....");
        List<String> command = Lists.newArrayList();
        if (config.useSudo()) {
            logger.info("Configured to use sudo to stop C*");

            if (!"root".equals(System.getProperty("user.name"))) {
                command.add(SUDO_STRING);
                command.add("-n");
                command.add("-E");
            }
        }
        for (String param : config.getCassStopScript().split(" ")) {
            if (StringUtils.isNotBlank(param)) command.add(param);
        }
        ProcessBuilder stopCass = new ProcessBuilder(command);
        stopCass.directory(new File("/"));
        stopCass.redirectErrorStream(true);

        instanceState.setShouldCassandraBeAlive(false);
        if (!force && config.getGracefulDrainHealthWaitSeconds() >= 0) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future drainFuture =
                    executor.submit(
                            () -> {
                                // As the node has been marked as shutting down above in
                                // setShouldCassandraBeAlive, we wait this
                                // duration to allow external healthcheck systems time to pick up
                                // the state change.
                                try {
                                    Thread.sleep(config.getGracefulDrainHealthWaitSeconds() * 1000);
                                } catch (InterruptedException e) {
                                    return;
                                }

                                try {
                                    JMXNodeTool nodetool = JMXNodeTool.instance(config);
                                    nodetool.drain();
                                } catch (InterruptedException
                                        | IOException
                                        | ExecutionException e) {
                                    logger.error(
                                            "Exception draining Cassandra, could not drain. Proceeding with shutdown.",
                                            e);
                                }
                                // Once Cassandra is drained the thrift/native servers are shutdown
                                // and there is no need to wait to
                                // stop Cassandra. Just stop it now.
                            });

            // In case drain hangs, timeout the future and continue stopping anyways. Give drain 30s
            // always
            // In production we frequently see servers that do not want to drain
            try {
                drainFuture.get(config.getGracefulDrainHealthWaitSeconds() + 30, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException | InterruptedException e) {
                logger.error(
                        "Waited 30s for drain but it did not complete, continuing to shutdown", e);
            }
        }
        Process stopper = stopCass.start();
        try {
            int code = stopper.waitFor();
            if (code == 0) {
                logger.info("Cassandra server has been stopped");
                this.cassMonitorMetrics.incCassStop();
                instanceState.setCassandraProcessAlive(false);
            } else {
                logger.error("Unable to stop cassandra server. Error code: {}", code);
                logProcessOutput(stopper);
            }
        } catch (Exception e) {
            logger.warn("couldn't shut down cassandra correctly", e);
        }
    }
}
