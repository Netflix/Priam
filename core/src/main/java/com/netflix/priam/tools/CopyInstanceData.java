package com.netflix.priam.tools;

import com.google.common.collect.Ordering;
import com.netflix.priam.aws.DefaultCredentials;
import com.netflix.priam.aws.SDBInstanceData;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.PriamInstance;
import com.yammer.dropwizard.AbstractService;
import com.yammer.dropwizard.cli.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

/**
 * Copy simple db data for a particular cluster from one AWS region to another.  This can be useful when migrating a
 * live cluster that used to store simple db data in us-east-1 but now wants to store it in the local region for better
 * performance and cross-data center isolation.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class CopyInstanceData extends Command {

    public CopyInstanceData() {
        super("copy-instance-data", "Copies SimpleDB instance data from one region to another.");
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption("c", "cluster", true, "Cassandra cluster name");
        options.addOption("d", "domain", true, "AWS SimpleDB domain");
        options.addOption(null, "src-region", true, "AWS SimpleDB source region");
        options.addOption(null, "dest-region", true, "AWS SimpleDB destination region");
        return options;
    }

    @Override
    protected void run(AbstractService<?> service, CommandLine cmdLine) throws Exception {
        if (!cmdLine.getArgList().isEmpty()) {
            printHelp("Unexpected command-line argument.", service.getClass());
            System.exit(2);
        }
        String cluster = getRequiredOption("cluster", cmdLine, service);
        String domain = getRequiredOption("domain", cmdLine, service);
        String srcRegion = getRequiredOption("src-region", cmdLine, service);
        String destRegion = getRequiredOption("dest-region", cmdLine, service);

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

    private SDBInstanceData getSimpleDB(String domain, String region) {
        AmazonConfiguration awsConfig = new AmazonConfiguration();
        awsConfig.setSimpleDbDomain(domain);
        awsConfig.setSimpleDbRegion(region);
        return new SDBInstanceData(new DefaultCredentials(), awsConfig);
    }

    private String getRequiredOption(String name, CommandLine cmdLine, AbstractService<?> service) {
        if (!cmdLine.hasOption(name)) {
            printHelp(format("--%s argument is required.", name), service.getClass());
            System.exit(2);
        }
        return cmdLine.getOptionValue(name);
    }
}
