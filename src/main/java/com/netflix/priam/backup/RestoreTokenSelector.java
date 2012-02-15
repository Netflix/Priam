package com.netflix.priam.backup;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.google.inject.Inject;
import com.netflix.priam.utils.TokenManager;

/**
 * Runs algorithms as finding closest token from a list of token (in a backup)
 */
public class RestoreTokenSelector
{
    protected final IBackupFileSystem fs;

    @Inject
    public RestoreTokenSelector(IBackupFileSystem fs)
    {
        this.fs = fs;
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
        Iterator<AbstractBackupPath> iter = fs.listPrefixes(startdate);
        while(iter.hasNext())
            tokenList.add(new BigInteger(iter.next().getToken()));
        return TokenManager.findClosestToken(tokenToSearch, tokenList);
    }
}
