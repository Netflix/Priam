/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
