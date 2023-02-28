package com.netflix.priam.health;

import com.netflix.priam.config.IConfiguration;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftChecker implements IThriftChecker {
    private static final Logger logger = LoggerFactory.getLogger(ThriftChecker.class);
    protected final IConfiguration config;

    @Inject
    public ThriftChecker(IConfiguration config) {
        this.config = config;
    }

    public boolean isThriftServerListening() {
        if (!config.checkThriftServerIsListening()) {
            return true;
        }
        String[] cmd =
                new String[] {
                    "/bin/sh", "-c", "ss -tuln | grep -c " + config.getThriftPort(), " 2>/dev/null"
                };
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(cmd);
            process.waitFor(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("Exception while executing the process: ", e);
        }
        if (process != null) {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream())); ) {
                if (Integer.parseInt(reader.readLine()) == 0) {
                    logger.info(
                            "Could not find anything listening on the rpc port {}!",
                            config.getThriftPort());
                    return false;
                }
            } catch (Exception e) {
                logger.warn("Exception while reading the input stream: ", e);
            }
        }
        // A quiet on-call is our top priority, err on the side of avoiding false positives by
        // default.
        return true;
    }
}
