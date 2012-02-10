package com.netflix.priam.backup;

import java.math.BigInteger;
import java.util.Date;

public interface IRestoreTokenSelector
{
    /**
     * Get the closest token to current token from the list of tokens available
     * in the backup
     * 
     * @param tokenToSearch
     * @param startDate
     * @return
     */
    public BigInteger getClosestToken(BigInteger tokenToSearch, Date startDate);
}
