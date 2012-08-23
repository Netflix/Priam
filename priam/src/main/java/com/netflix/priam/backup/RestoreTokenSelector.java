package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.netflix.priam.utils.ITokenManager;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Runs algorithms as finding closest token from a list of token (in a backup)
 */
public class RestoreTokenSelector
{
    private final IBackupFileSystem fs;
    private final ITokenManager tokenManager;

    @Inject
    public RestoreTokenSelector(IBackupFileSystem fs, ITokenManager tokenManager)
    {
        this.fs = fs;
        this.tokenManager = tokenManager;
    }

    /**
     * Get the closest token to current token from the list of tokens available
     * in the backup
     * 
     * @param tokenToSearch
     *            Token to search for
     * @param startDate
     *            Date for which the backups are available
     * @return Token as BigInteger
     */
    public BigInteger getClosestToken(BigInteger tokenToSearch, Date startDate)
    {
        List<BigInteger> tokenList = new ArrayList<BigInteger>();
        Iterator<AbstractBackupPath> iter = fs.listPrefixes(startDate);
        while (iter.hasNext())
            tokenList.add(new BigInteger(iter.next().getToken()));
        return tokenManager.findClosestToken(tokenToSearch, tokenList);
    }
}
