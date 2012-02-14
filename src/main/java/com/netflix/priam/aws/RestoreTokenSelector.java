package com.netflix.priam.aws;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.IRestoreTokenSelector;
import com.netflix.priam.utils.TokenManager;

/**
 * Runs algorithms as finding closest token from a list of token (in a backup)
 */
public class RestoreTokenSelector implements IRestoreTokenSelector
{
    public static final char PATH_SEP = '/';

    protected final IConfiguration config;
    protected final AmazonS3 s3Client;

    private static final Logger logger = LoggerFactory.getLogger(RestoreTokenSelector.class);
    private String bucket = "";
    private String prefix = "";

    @Inject
    public RestoreTokenSelector(IConfiguration config, ICredential provider)
    {
        this.config = config;
        String path = "";
        if (!"".equals(config.getRestorePrefix()))
            path = config.getRestorePrefix();
        else
            path = config.getBackupPrefix();

        String[] paths = path.split(String.valueOf(S3BackupPath.PATH_SEP));
        bucket = paths[0];
        this.prefix = remotePrefix(path);
        AWSCredentials cred = new BasicAWSCredentials(provider.getAccessKeyId(), provider.getSecretAccessKey());
        s3Client = new AmazonS3Client(cred);
    }

    @Override
    public BigInteger getClosestToken(BigInteger tokenToSearch, Date startdate)
    {
        SimpleDateFormat datefmt = new SimpleDateFormat("yyyyMMdd");
        List<BigInteger> tokenList = getTokenList(datefmt.format(startdate));
        return TokenManager.findClosestToken(tokenToSearch, tokenList);
    }

    private String remotePrefix(String location)
    {
        StringBuffer buff = new StringBuffer();
        String[] elements = location.split(String.valueOf(S3BackupPath.PATH_SEP));
        if (elements.length <= 1)
        {
            buff.append(config.getBackupLocation()).append(S3BackupPath.PATH_SEP);
            buff.append(config.getDC()).append(S3BackupPath.PATH_SEP);
            buff.append(config.getAppName()).append(S3BackupPath.PATH_SEP);
        }
        else
        {
            assert elements.length >= 4 : "Too few elements in path " + location;
            buff.append(elements[1]).append(S3BackupPath.PATH_SEP);
            buff.append(elements[2]).append(S3BackupPath.PATH_SEP);
            buff.append(elements[3]).append(S3BackupPath.PATH_SEP);
        }
        return buff.toString();
    }

    private List<BigInteger> getTokenList(String date)
    {
        Iterator<String> tokenPaths = tokenIterator(prefix);
        List<BigInteger> tokens = new ArrayList<BigInteger>();
        while (tokenPaths.hasNext())
        {
            String tokenPath = tokenPaths.next();
            if (tokenExistsForDate(tokenPath, date))
            {
                String[] splits = tokenPath.split("/");
                logger.info("Token found: " + splits[3]);
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
        logger.info("Using cluster prefix for searching tokens: " + clusterPath);
        ObjectListing listing;
        do
        {
            listing = s3Client.listObjects(listReq);
            for (String summary : listing.getCommonPrefixes())
                tokenList.add(summary);

        } while (listing.isTruncated());
        return tokenList.iterator();
    }

    private boolean tokenExistsForDate(String tprefix, String datestr)
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
}
