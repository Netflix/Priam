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
package com.netflix.priam.restore;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.ITokenManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(RestoreTokenSelector.class);
    private final ITokenManager tokenManager;
    private final IBackupFileSystem fs;
    private final IPriamInstanceFactory factory;

    @Inject

    public RestoreTokenSelector(ITokenManager tokenManager,@Named("backup")IBackupFileSystem fs, IPriamInstanceFactory factory)

    {
        this.tokenManager = tokenManager;
        this.fs = fs;
        this.factory = factory;
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
    public BigInteger getClosestToken(String tokenToSearch, Date startDate)
    {
        BigInteger tokenToSearchInteger = parseToken(tokenToSearch, "Instance does not have a valid, single token to find the closest backup to. Token: %s");
        List<BigInteger> tokenList = new ArrayList<BigInteger>();
        Iterator<AbstractBackupPath> iter = fs.listPrefixes(startDate);
        while (iter.hasNext()) {
            BigInteger backupToken = parseToken(iter.next().getNodeIdentifier(), "Backup is not identified by a token. BackupNodeIdentifier: %s");
            tokenList.add(backupToken);
        }
        return tokenManager.findClosestToken(tokenToSearchInteger, tokenList);
    }

    /**
     * Get the closest token to current token among existing nodes in the specified region
     *
     * @param tokenToSearch
     *            Token to search for
     * @param appName
     *            the cluster name
     * @param region
     *            Region to search for a node with the closest token
     * @return Token as BigInteger
     */
    public BigInteger getClosestToken(String tokenToSearch, String appName, String region)
    {
        BigInteger tokenToSearchInteger = parseToken(tokenToSearch, "Instance does not have a valid, single token to find the closest backup to. Token: %s");
        List<PriamInstance> plist = factory.getAllIds(appName);
        List<BigInteger> tokenList = Lists.newArrayList();
        for (PriamInstance ins : plist)
        {
            if (ins.getDC().equalsIgnoreCase(region)) {
                BigInteger otherNodeToken = parseToken(ins.getToken(), "Remote instance does not have a valid, single token to find the closest backup to. Token: %s");
                tokenList.add(otherNodeToken);
            }
        }
        return tokenManager.findClosestToken(tokenToSearchInteger, tokenList);
    }

    private BigInteger parseToken(String tokenString, String errorMessageFormat)
    {
        try {
            return new BigInteger(tokenString);
        }
        catch (NumberFormatException ex) {
            logger.error(String.format(errorMessageFormat, tokenString), ex);
            throw ex;
        }
    }
}