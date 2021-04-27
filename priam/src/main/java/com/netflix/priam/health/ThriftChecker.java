package com.netflix.priam.health;

import com.google.inject.Inject;
import com.netflix.priam.config.IConfiguration;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
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
        try {
            Process process =
                    Runtime.getRuntime()
                            .exec(
                                    new String[] {
                                        "/bin/sh",
                                        "-c",
                                        "ss -tuln | grep -c " + config.getThriftPort(),
                                        " 2>/dev/null"
                                    });
            process.waitFor(1, TimeUnit.SECONDS);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = reader.readLine();
            reader.close();
            if (Integer.parseInt(line) == 0) {
                logger.info(
                        "Could not find anything listening on the rpc port {}!",
                        config.getThriftPort());
                return false;
            }
        } catch (Exception e) {
            logger.warn("Exception thrown while checking if process is listening on a port ", e);
        }
        // Returning true for exceptions as well because we do not want to generate unnecessary
        // noise that thrift is not listening on the port when actually we are unable to execute the
        // query.
        return true;
    }
}
