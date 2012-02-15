package com.netflix.priam.backup;

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
import com.netflix.priam.utils.TokenManager;

/**
 * Runs algorithms as finding closest token from a list of token (in a backup)
 */
public class RestoreTokenSelector
{
    protected final IConfiguration config;
    protected final IBackupFileSystem fs;

    private String prefix = "";

    @Inject
    public RestoreTokenSelector(IConfiguration config, IBackupFileSystem fs)
    {
        this.config = config;
        this.fs = fs;
        prefix = "";
        if (!"".equals(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();
    }

    /**
     * Get the closest token to current token from the list of tokens available
     * in the backup
     * 
     * @param tokenToSearch Token to search for
     * @param startDate     Date for which the backups are available
     * @return
     */
    public BigInteger getClosestToken(BigInteger tokenToSearch, Date startdate)
    {
        List<BigInteger> tokenList = new ArrayList<BigInteger>();
        Iterator<BigInteger> iter = fs.tokenIterator(prefix, startdate);
        while(iter.hasNext())
            tokenList.add(iter.next());
        return TokenManager.findClosestToken(tokenToSearch, tokenList);
    }
}
