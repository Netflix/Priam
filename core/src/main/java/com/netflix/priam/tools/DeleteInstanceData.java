package com.netflix.priam.tools;

import ch.qos.logback.classic.Level;
import com.netflix.priam.aws.DefaultCredentials;
import com.netflix.priam.aws.SDBInstanceData;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.PriamInstance;
import com.yammer.dropwizard.config.LoggingConfiguration;
import com.yammer.dropwizard.config.LoggingFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Deletes simple db data based on the numeric priam instance IDs.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class DeleteInstanceData {

    public static void main(String[] args) throws ParseException {
        LoggingConfiguration logConfig = new LoggingConfiguration();
        logConfig.getConsoleConfiguration().setThreshold(Level.WARN);
        new LoggingFactory(logConfig, "deleteInstanceData").configure();

        Options options = new Options();
        options.addOption("c", "cluster", true, "Cassandra cluster name");
        options.addOption("d", "domain", true, "AWS SimpleDB domain");
        options.addOption("r", "region", true, "AWS SimpleDB region");
        CommandLine cmdLine = new GnuParser().parse(options, args);

        String cluster = checkNotNull(cmdLine.getOptionValue("cluster"), "--cluster is required");
        String domain = checkNotNull(cmdLine.getOptionValue("domain"), "--domain is required");
        String region = cmdLine.getOptionValue("region");
        String[] ids = cmdLine.getArgs();

        SDBInstanceData sdb = getSimpleDB(domain, region);

        for (String id : ids) {
            PriamInstance instance = sdb.getInstance(cluster, Integer.parseInt(id));
            if (instance == null) {
                System.err.println("No priam instance with id " + id + " found.");
                continue;
            }
            sdb.deregisterInstance(instance);
            System.out.println("Deleted: " + id);
        }
    }

    private static SDBInstanceData getSimpleDB(String domain, String region) {
        AmazonConfiguration awsConfig = new AmazonConfiguration();
        awsConfig.setSimpleDbDomain(domain);
        awsConfig.setSimpleDbRegion(region);
        return new SDBInstanceData(new DefaultCredentials(), awsConfig);
    }
}
