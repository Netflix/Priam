package com.netflix.priam.tools;

import com.google.common.collect.Ordering;
import com.netflix.priam.aws.DefaultCredentials;
import com.netflix.priam.aws.SDBInstanceData;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.PriamInstance;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * Print simple db data for a particular cluster to stdout.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class ListInstanceData extends Command {

    public ListInstanceData() {
        super("list-instance-data", "Lists SimpleDB instance data for a particular Cassandra cluster.");
    }

    @Override
    public void configure(Subparser subparser) {
        subparser.addArgument("-c", "--cluster").required(true).help("Cassandra cluster name");
        subparser.addArgument("-d", "--domain").required(true).help("AWS SimpleDB domain");
        subparser.addArgument("-r", "--region").required(false).help("AWS SimpleDB region");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        String cluster = namespace.getString("cluster");
        String domain = namespace.getString("domain");
        String region = namespace.getString("region");

        SDBInstanceData sdb = getSimpleDB(domain, region);

        for (PriamInstance id : Ordering.natural().sortedCopy(sdb.getAllIds(cluster))) {
            System.out.println(id);
        }
    }

    private static SDBInstanceData getSimpleDB(String domain, String region) {
        AmazonConfiguration awsConfig = new AmazonConfiguration();
        awsConfig.setSimpleDbDomain(domain);
        awsConfig.setSimpleDbRegion(region);
        return new SDBInstanceData(new DefaultCredentials(), awsConfig);
    }
}
