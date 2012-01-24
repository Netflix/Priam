package com.priam.backup;

import java.math.BigInteger;
import java.util.Date;

public interface IRestoreTokenSelector
{
    public BigInteger getClosestToken(BigInteger tokenToSearch, Date startDate);
}
