package com.netflix.priam.tools;

import com.google.common.collect.Ordering;
import com.netflix.priam.aws.DefaultCredentials;
import com.netflix.priam.aws.SDBInstanceData;
import com.netflix.priam.config.AmazonConfiguration;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

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
    public void configure(Subparser subparser) {
        subparser.addArgument("-d", "--domain").required(true).help("AWS SimpleDB domain");
        subparser.addArgument("-r", "--region").required(false).help("AWS SimpleDB region");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        String domain = namespace.getString("domain");
        String region = namespace.getString("region");

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
}
