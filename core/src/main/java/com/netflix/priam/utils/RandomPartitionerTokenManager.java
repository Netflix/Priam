package com.netflix.priam.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.cassandra.dht.RandomPartitioner;

import java.math.BigInteger;
import java.util.List;

public class RandomPartitionerTokenManager extends TokenManager {
    /**
     * Calculate a token for the given position, evenly spaced from other size-1 nodes.  See
     * http://wiki.apache.org/cassandra/Operations.
     *
     * @param size     number of slots by which the token space will be divided
     * @param position slot number, multiplier
     * @param offset   added to token
     * @return MAXIMUM_TOKEN / size * position + offset, if <= MAXIMUM_TOKEN, otherwise wrap around the MINIMUM_TOKEN
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
        return RandomPartitioner.MAXIMUM.divide(BigInteger.valueOf(size))
                .multiply(BigInteger.valueOf(position))
                .add(BigInteger.valueOf(offset));
    }

    @Override
    public String createToken(int mySlot, int totalCount, String region) {
        return initialToken(totalCount, mySlot, regionOffset(region)).toString();
    }

    @Override
    public String findClosestToken(String tokenStringToSearch, List<String> tokenList) {
        Preconditions.checkArgument(!tokenList.isEmpty(), "token list must not be empty");
        BigInteger tokenToSearch = new BigInteger(tokenStringToSearch);
        List<BigInteger> sortedTokens = Ordering.natural().sortedCopy(Lists.transform(tokenList, new Function<String, BigInteger>() {
            @Override
            public BigInteger apply(String token) {
                return new BigInteger(token);
            }
        }));
        int index = Ordering.natural().binarySearch(sortedTokens, tokenToSearch);
        if (index < 0) {
            int i = -index - 1;
            if ((i >= sortedTokens.size()) ||
                    (i > 0 && sortedTokens.get(i).subtract(tokenToSearch).compareTo(tokenToSearch.subtract(sortedTokens.get(i - 1))) > 0)) {
                --i;
            }
            return sortedTokens.get(i).toString();
        }
        return sortedTokens.get(index).toString();
    }
}
