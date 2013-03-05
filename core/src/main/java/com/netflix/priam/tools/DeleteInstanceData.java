package com.netflix.priam.tools;

import com.netflix.priam.aws.DefaultCredentials;
import com.netflix.priam.aws.SDBInstanceData;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.PriamInstance;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.util.List;

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
    public void configure(Subparser subparser) {
        subparser.addArgument("-c", "--cluster").required(true).help("Cassandra cluster name");
        subparser.addArgument("-d", "--domain").required(true).help("AWS SimpleDB domain");
        subparser.addArgument("-r", "--region").required(false).help("AWS SimpleDB region");
        subparser.addArgument("instance-id").nargs("+").help("Priam instance IDs");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        String cluster = namespace.getString("cluster");
        String domain = namespace.getString("domain");
        String region = namespace.getString("region");
        List<String> ids = namespace.getList("instance-id");

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
