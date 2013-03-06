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
    public void configure(Subparser subparser) {
        subparser.addArgument("-c", "--cluster").required(true).help("Cassandra cluster name");
        subparser.addArgument("-d", "--domain").required(true).help("AWS SimpleDB domain");
        subparser.addArgument("--src-region").required(true).help("AWS SimpleDB source region");
        subparser.addArgument("--dest-region").required(true).help("AWS SimpleDB destination region");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        String cluster = namespace.getString("cluster");
        String domain = namespace.getString("domain");
        String srcRegion = namespace.getString("src-region");
        String destRegion = namespace.getString("dest-region");

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
}
