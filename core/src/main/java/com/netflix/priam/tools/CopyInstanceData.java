package com.netflix.priam.tools;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Ordering;
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
 * Copy simple db data for a particular cluster from one AWS region to another.  This can be useful when migrating a
 * live cluster that used to store simple db data in us-east-1 but now wants to store it in the local region for better
 * performance and cross-data center isolation.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class CopyInstanceData {

    public static void main(String[] args) throws ParseException {
        LoggingConfiguration logConfig = new LoggingConfiguration();
        logConfig.getConsoleConfiguration().setThreshold(Level.WARN);
        new LoggingFactory(logConfig, "copyInstanceData").configure();

        Options options = new Options();
        options.addOption("c", "cluster", true, "Cassandra cluster name");
        options.addOption("d", "domain", true, "AWS SimpleDB domain");
        options.addOption(null, "src-region", true, "AWS SimpleDB source region");
        options.addOption(null, "dest-region", true, "AWS SimpleDB destination region");
        CommandLine cmdLine = new GnuParser().parse(options, args);

        String cluster = checkNotNull(cmdLine.getOptionValue("cluster"), "--cluster is required");
        String domain = checkNotNull(cmdLine.getOptionValue("domain"), "--domain is required");
        String srcRegion = checkNotNull(cmdLine.getOptionValue("src-region"), "--src-region is required");
        String destRegion = checkNotNull(cmdLine.getOptionValue("dest-region"), "--dest-region is required");

        SDBInstanceData srcSdb = getSimpleDB(domain, srcRegion);
        SDBInstanceData destSdb = getSimpleDB(domain, destRegion);

        for (PriamInstance id : Ordering.natural().sortedCopy(srcSdb.getAllIds(cluster))) {
            System.out.println("Copying " + id + "...");
            try {
                destSdb.createInstance(id);
            } catch (Exception e) {
                System.err.println("Copy failed for " + id + ":" + e);
            }
        }
    }

    private static SDBInstanceData getSimpleDB(String domain, String region) {
        AmazonConfiguration awsConfig = new AmazonConfiguration();
        awsConfig.setSimpleDbDomain(domain);
        awsConfig.setSimpleDbRegion(region);
        return new SDBInstanceData(new DefaultCredentials(), awsConfig);
    }
}
