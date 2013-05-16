package com.netflix.priam.cassandra;

import com.netflix.priam.utils.SystemUtils;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class NFThinCassandraDaemon extends CassandraDaemon {
    private static final Logger logger = LoggerFactory.getLogger(NFThinCassandraDaemon.class);

    /**
     * Initialize the Cassandra Daemon based on the given command-line
     * arguments. This is the normal {@code java} entry point.
     *
     * @param args the arguments passed on the command-line
     */
    public static void main(String[] args) {
        NFThinCassandraDaemon daemon = new NFThinCassandraDaemon();
        daemon.setPriamProperties();
        daemon.activate();
    }

    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for
     * JSVC and serves as a second entry point.
     *
     * @param args the arguments passed in from JSVC
     * @throws IOException
     */
    @Override
    public void init(String[] args) throws IOException {
        setPriamProperties();
        protectFromOomKiller();
        super.init(args);
    }

    private void setPriamProperties() {
        String token = null;
        String seeds = null;
        boolean isReplace = false;
        while (true) {
            try {
                token = SystemUtils.getDataFromUrl("http://127.0.0.1:8080/v1/cassconfig/get_token");
                seeds = SystemUtils.getDataFromUrl("http://127.0.0.1:8080/v1/cassconfig/get_seeds");
                isReplace = Boolean.parseBoolean(SystemUtils.getDataFromUrl("http://127.0.0.1:8080/v1/cassconfig/is_replace_token"));
            } catch (Exception e) {
                logger.error("Failed to obtain a token from a pre-defined list, we can not start!", e);
            }

            if (token != null && seeds != null) {
                break;
            }
            // sleep for 5 sec and try again.
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e1) {
                // do nothing.
            }
        }

        System.setProperty("cassandra.initial_token", token);

        if (isReplace) {
            System.setProperty("cassandra.replace_token", token);
        }
    }

    private void protectFromOomKiller() {
        File oomAdj = new File("/proc/self/oom_adj");

        if (!oomAdj.exists()) {
            return;
        }

        try {
            BufferedWriter oomAdjWriter = new BufferedWriter(new FileWriter(oomAdj));
            // oom_adj ranges from -17 to 15
            oomAdjWriter.write("-16");
            oomAdjWriter.close();
        } catch (IOException e) {
            logger.warn("Failed to write OOM adjust.", e);
        }
    }
}
