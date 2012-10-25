package com.netflix.priam.tools;

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
 * Deletes simple db data based on the numeric priam instance IDs.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class DeleteInstanceData extends Command {

    public DeleteInstanceData() {
        super("delete-instance-data", "Deletes SimpleDB instance data from a region.");
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption("c", "cluster", true, "Cassandra cluster name");
        options.addOption("d", "domain", true, "AWS SimpleDB domain");
        options.addOption("r", "region", true, "AWS SimpleDB region");
        return options;
    }

    @Override
    protected void run(AbstractService<?> service, CommandLine cmdLine) throws Exception {
        if (cmdLine.getArgList().isEmpty()) {
            printHelp("Expected at least one Priam instance id.", service.getClass());
            System.exit(2);
        }
        String cluster = getRequiredOption("cluster", cmdLine, service);
        String domain = getRequiredOption("domain", cmdLine, service);
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

    private String getRequiredOption(String name, CommandLine cmdLine, AbstractService<?> service) {
        if (!cmdLine.hasOption(name)) {
            printHelp(format("--%s argument is required.", name), service.getClass());
            System.exit(2);
        }
        return cmdLine.getOptionValue(name);
    }
}
