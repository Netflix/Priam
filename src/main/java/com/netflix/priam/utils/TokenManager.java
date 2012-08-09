package com.netflix.priam.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import java.math.BigInteger;
import java.util.List;

public class TokenManager {
    public static final BigInteger MINIMUM_TOKEN = new BigInteger("0");
    public static final BigInteger MAXIMUM_TOKEN = new BigInteger("2").pow(127);

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
    static BigInteger initialToken(int size, int position, int offset) {
        Preconditions.checkArgument(size > 0, "size must be > 0");
        Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
        /*
         * TODO: Is this it valid to add "&& position < size" to the following precondition?  This currently causes
         * unit test failures.
         */
        Preconditions.checkArgument(position >= 0, "position must be >= 0");
        return MAXIMUM_TOKEN.divide(BigInteger.valueOf(size))
                .multiply(BigInteger.valueOf(position))
                .add(BigInteger.valueOf(offset));
    }

    /**
     * Creates a token given the following parameter
     *
     * @param mySlot   -- Slot where this instance has to be.
     * @param availabilityZones -- The number of AvailabilityZones
     * @param availabilityZoneMembershipSize  -- number of members in the availabilityZone
     * @param region    -- name of the DC where it this token is created.
     */
    public static String createToken(int mySlot, int availabilityZones, int availabilityZoneMembershipSize, String region) {
        int regionCount = availabilityZones * availabilityZoneMembershipSize;
        return initialToken(regionCount, mySlot, regionOffset(region)).toString();
    }

    public static String createToken(int mySlot, int totalCount, String region) {
        return initialToken(totalCount, mySlot, regionOffset(region)).toString();
    }

    public static BigInteger findClosestToken(BigInteger tokenToSearch, List<BigInteger> tokenList) {
        Preconditions.checkArgument(!tokenList.isEmpty(), "token list must not be empty");
        List<BigInteger> sortedTokens = Ordering.natural().sortedCopy(tokenList);
        int index = Ordering.natural().binarySearch(sortedTokens, tokenToSearch);
        if (index < 0) {
            int i = Math.abs(index) - 1;
            if ((i >= sortedTokens.size()) || (i > 0 && sortedTokens.get(i).subtract(tokenToSearch)
                    .compareTo(tokenToSearch.subtract(sortedTokens.get(i - 1))) > 0)) {
                --i;
            }
            return sortedTokens.get(i);
        }
        return sortedTokens.get(index);
    }

    /**
     * Create an offset to add to token values by hashing the region name.
     */
    public static int regionOffset(String region) {
        return Math.abs(region.hashCode());
    }
}
