/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.netflix.priam.config.IConfiguration;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

public class TokenManager implements ITokenManager {
    public static final BigInteger MINIMUM_TOKEN_RANDOM = BigInteger.ZERO;
    public static final BigInteger MAXIMUM_TOKEN_RANDOM = new BigInteger("2").pow(127);
    public static final BigInteger MINIMUM_TOKEN_MURMUR3 = new BigInteger("-2").pow(63);
    public static final BigInteger MAXIMUM_TOKEN_MURMUR3 = new BigInteger("2").pow(63);

    private final BigInteger minimumToken;
    private final BigInteger maximumToken;
    private final BigInteger tokenRangeSize;

    private final IConfiguration config;

    @Inject
    public TokenManager(IConfiguration config) {
        this.config = config;

        if ("org.apache.cassandra.dht.Murmur3Partitioner".equals(this.config.getPartitioner())) {
            minimumToken = MINIMUM_TOKEN_MURMUR3;
            maximumToken = MAXIMUM_TOKEN_MURMUR3;
        } else {
            minimumToken = MINIMUM_TOKEN_RANDOM;
            maximumToken = MAXIMUM_TOKEN_RANDOM;
        }
        tokenRangeSize = maximumToken.subtract(minimumToken);
    }

    /**
     * Calculate a token for the given position, evenly spaced from other size-1 nodes. See
     * http://wiki.apache.org/cassandra/Operations.
     *
     * @param size number of slots by which the token space will be divided
     * @param position slot number, multiplier
     * @param offset added to token
     * @return MAXIMUM_TOKEN / size * position + offset, if <= MAXIMUM_TOKEN, otherwise wrap around
     *     the MINIMUM_TOKEN
     */
    @VisibleForTesting
    BigInteger initialToken(int size, int position, int offset) {
        Preconditions.checkArgument(size > 0, "size must be > 0");
        Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
        /*
         * TODO: Is this it valid to add "&& position < size" to the following precondition?  This currently causes
         * unit test failures.
         */
        Preconditions.checkArgument(position >= 0, "position must be >= 0");
        return tokenRangeSize
                .divide(BigInteger.valueOf(size))
                .multiply(BigInteger.valueOf(position))
                .add(BigInteger.valueOf(offset))
                .add(minimumToken);
    }

    @Override
    public String createToken(int my_slot, int totalCount, String region) {
        return initialToken(totalCount, my_slot, regionOffset(region)).toString();
    }

    @Override
    public BigInteger findClosestToken(BigInteger tokenToSearch, List<BigInteger> tokenList) {
        Preconditions.checkArgument(!tokenList.isEmpty(), "token list must not be empty");
        List<BigInteger> sortedTokens = Ordering.natural().sortedCopy(tokenList);
        int index = Collections.binarySearch(sortedTokens, tokenToSearch, Ordering.natural());
        if (index < 0) {
            int i = Math.abs(index) - 1;
            if ((i >= sortedTokens.size())
                    || (i > 0
                            && sortedTokens
                                            .get(i)
                                            .subtract(tokenToSearch)
                                            .compareTo(
                                                    tokenToSearch.subtract(sortedTokens.get(i - 1)))
                                    > 0)) --i;
            return sortedTokens.get(i);
        }
        return sortedTokens.get(index);
    }

    /** Create an offset to add to token values by hashing the region name. */
    @Override
    public int regionOffset(String region) {
        return Math.abs(region.hashCode());
    }
}
