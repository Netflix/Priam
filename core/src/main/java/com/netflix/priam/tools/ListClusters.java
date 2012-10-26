package com.netflix.priam.tools;

import com.google.common.collect.Ordering;
import com.netflix.priam.aws.DefaultCredentials;
import com.netflix.priam.aws.SDBInstanceData;
import com.netflix.priam.config.AmazonConfiguration;
import com.yammer.dropwizard.AbstractService;
import com.yammer.dropwizard.cli.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

/**
 * Print simple db data summary of clusters to stdout.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class ListClusters extends Command {

    public ListClusters() {
        super("list-clusters", "Lists Cassandra clusters in a particular SimpleDB domain.");
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption("d", "domain", true, "AWS SimpleDB domain");
        options.addOption("r", "region", true, "AWS SimpleDB region");
        return options;
    }

    @Override
    protected void run(AbstractService<?> service, CommandLine cmdLine) throws Exception {
        if (!cmdLine.getArgList().isEmpty()) {
            printHelp("Unexpected command-line argument.", service.getClass());
            System.exit(2);
        }
        String domain = getRequiredOption("domain", cmdLine, service);
        String region = cmdLine.getOptionValue("region");

        SDBInstanceData sdb = getSimpleDB(domain, region);

        for (String cluster : Ordering.natural().sortedCopy(sdb.getAllAppIds())) {
            System.out.println(cluster);
        }
    }

    private static SDBInstanceData getSimpleDB(String domain, String region) {
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
