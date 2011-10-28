package com.priam.netflix;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.netflix.aws.AWSManager;
import com.netflix.instance.identity.InstanceData;
import com.netflix.instance.identity.aws.InstanceDataDAOSimpleDb;
import com.netflix.library.NFLibraryManager;
import com.netflix.platform.core.PlatformManager;

/**
 * Utility to update the tokens in simpledb with tokens from a given backup
 * 
 * @author Praveen Sadhu
 * 
 */
public class BackPortRestoreToken
{
    private static final Logger logger = LoggerFactory.getLogger(BackPortRestoreToken.class);
    public static final char PATH_SEP = '/';
    private String bucket = "";
    private AmazonS3 s3Client;
    private String prefix = "";

    public BackPortRestoreToken(String prefix)
    {
        Properties props = new Properties();
        props.setProperty("platform.ListOfComponentsToInit", "LOGGING,APPINFO,AWS");
        props.setProperty("platform.ListOfMandatoryComponentsToInit", "");
        props.setProperty("netflix.environment", System.getenv("NETFLIX_ENVIRONMENT"));
        System.setProperty("netflix.logging.realtimetracers", "true");
        System.setProperty("netflix.appinfo.name", "nfcassandra.unittest");

        try
        {
            NFLibraryManager.initLibrary(PlatformManager.class, props, true, false);
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
        // this.prefix = StringUtils.strip(prefix,String.valueOf(PATH_SEP)) ;
        this.prefix = prefix;
        this.prefix = this.prefix.substring(this.prefix.indexOf(PATH_SEP) + 1);
        String[] splits = prefix.split("/");
        this.bucket = splits[0];
        AWSCredentials cred = new BasicAWSCredentials(AWSManager.getInstance().getAccessKeyId(), AWSManager.getInstance().getSecretAccessKey());
        s3Client = new AmazonS3Client(cred);

    }

    public void port(String appName, String datestr, boolean dryrun) throws Exception
    {
        List<BigInteger> tokens = getTokenList(datestr);
        logger.info("FOUND " + tokens.size() + " tokens");
        Collections.sort(tokens);
        for (BigInteger token : tokens)
        {
            logger.info("Got token: " + token);
        }

        // Get tokens from sdb
        List<InstanceData> idata = new ArrayList<InstanceData>();
        idata.addAll(InstanceDataDAOSimpleDb.getInstance().getAllIds(appName));

        Collections.sort(idata, new Comparator<InstanceData>()
        {

            @Override
            public int compare(InstanceData arg0, InstanceData arg1)
            {
                BigInteger b0 = new BigInteger(arg0.getPayload());
                BigInteger b1 = new BigInteger(arg1.getPayload());
                return b0.compareTo(b1);
            }

        });

        if (idata.size() != tokens.size())
            throw new Exception(String.format("Backup token count (%d) does not match simpledb token count(%d)", tokens.size(), idata.size()));
        Iterator<InstanceData> sdbiter = idata.iterator();
        Iterator<BigInteger> bkiter = tokens.iterator();
        while (sdbiter.hasNext() && bkiter.hasNext())
        {
            BigInteger newtoken = bkiter.next();
            InstanceData data = sdbiter.next();
            logger.info(String.format("MOVE: %s %s %s --> %s", data.getAvailabilityZone(), data.getInstanceId(), data.getPayload(), newtoken.toString()));
            if (!dryrun)
            {
                InstanceDataDAOSimpleDb.getInstance().deleteInstanceEntry(data);
                data.setPayload(newtoken.toString());
                InstanceDataDAOSimpleDb.getInstance().createInstanceEntry(data);
            }
        }
    }

    public List<BigInteger> getTokenList(String date)
    {
        Iterator<String> tokenPaths = tokenIterator(prefix);
        List<BigInteger> tokens = new ArrayList<BigInteger>();
        while (tokenPaths.hasNext())
        {
            String tokenPath = tokenPaths.next();
            if (isTokenAvailable(tokenPath, date))
            {
                String[] splits = tokenPath.split("/");
                tokens.add(new BigInteger(splits[3]));
            }
        }
        return tokens;
    }

    private Iterator<String> tokenIterator(String clusterPath)
    {
        ArrayList<String> tokenList = new ArrayList<String>();
        ListObjectsRequest listReq = new ListObjectsRequest();
        // Get list of tokens
        listReq.setBucketName(bucket);
        listReq.setPrefix(clusterPath);
        listReq.setDelimiter(String.valueOf(PATH_SEP));
        ObjectListing listing;
        do
        {
            listing = s3Client.listObjects(listReq);
            for (String summary : listing.getCommonPrefixes())
                tokenList.add(summary);

        } while (listing.isTruncated());
        return tokenList.iterator();
    }

    private boolean isTokenAvailable(String tprefix, String datestr)
    {
        ListObjectsRequest listReq = new ListObjectsRequest();
        // Get list of tokens
        listReq.setBucketName(bucket);
        listReq.setPrefix(tprefix + datestr);
        ObjectListing listing;
        listing = s3Client.listObjects(listReq);
        if (listing.getObjectSummaries().size() > 0)
            return true;
        return false;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        Options options = new Options();
        options.addOption("h", "help", false, "Help");
        options.addOption("p", "prefix", true, "Source prefix");
        options.addOption("A", "app_name", true, "Destination app name");
        options.addOption("D", "date_str", true, "Date str (YYYYMMDD)");
        options.addOption("d", "dryrun", false, "Dry run");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        if (args.length == 0 || cmd.hasOption("help"))
            BackPortRestoreToken.showUsageAndExit("", "", options);

        // delete specific key
        if (cmd.hasOption("p") && cmd.hasOption("A") && cmd.hasOption("D"))
        {
            BackPortRestoreToken bp = new BackPortRestoreToken(cmd.getOptionValue("p"));
            bp.port(cmd.getOptionValue("A"), cmd.getOptionValue("D"), cmd.hasOption('d'));
            return;
        }
        else
            BackPortRestoreToken.showUsageAndExit("Invalid command line args", "", options);
        System.exit(0);
    }

    public static void showUsageAndExit(String header, String footer, Options options)
    {
        HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp("BackPortRestoreToken", header, options, footer);
        System.exit(1);
    }

}
