package com.netflix.priam.utils;

import java.math.BigInteger;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

public class TokenManager
{    
    public static final BigInteger MINIMUM_TOKEN = new BigInteger("0");
    public static final BigInteger MAXIMUM_TOKEN = new BigInteger("2").pow(127);

    /**
     * Calculate a token for the given position, evenly spaced from other size-1 nodes.  See
     * http://wiki.apache.org/cassandra/Operations.
     *
     * @param size number of slots by which the token space will be divided
     * @param position slot number, multiplier
     * @param offset added to token
     * @return MAXIMUM_TOKEN / size * position + offset, if <= MAXIMUM_TOKEN, otherwise wrap around the MINIMUM_TOKEN
     */
    @VisibleForTesting static BigInteger initialToken(int size, int position, int offset)
    {
        Preconditions.checkArgument(size > 0, "size must be > 0");
        Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
        /*
         * TODO: Is this it valid to add "&& position < size" to the following precondition?  This currently causes
         * unit test failures.
         */
        Preconditions.checkArgument(position >= 0, "position must be >= 0");
        BigInteger decValue = MINIMUM_TOKEN;
        // (Maximum * max_slots * my_slot) -1
        if (position != 0)
            decValue = MAXIMUM_TOKEN.divide(new BigInteger("" + size)).multiply(new BigInteger("" + position)).subtract(new BigInteger("" + 1));
        // Add a Region/DC spacer to the token.
        decValue = decValue.add(new BigInteger("" + offset));
        // if the space is bigger then rotate to min for the ring.
        if (1 == decValue.compareTo(MAXIMUM_TOKEN))
        {
            BigInteger delta = decValue.subtract(MAXIMUM_TOKEN);
            decValue = MINIMUM_TOKEN.add(delta);
        }
        return decValue;
    }

    /**
     * Creates a token given the following parameter
     * 
     * @param my_slot
     *            -- Slot where this instance has to be.
     * @param rac_count
     *            -- Rac count is the numeber of RAC's
     * @param rac_size
     *            -- number of memberships in the rac
     * @param region
     *            -- name of the DC where it this token is created.
     */
    public static String createToken(int my_slot, int rac_count, int rac_size, String region)
    {
        int regionCount = rac_count * rac_size;
        return initialToken(regionCount, my_slot, regionOffset(region)).toString();
    }
    
    public static String createToken(int my_slot, int totalCount, String region)
    {
        return initialToken(totalCount, my_slot, regionOffset(region)).toString();
    }
    
    public static BigInteger findClosestToken(BigInteger tokenToSearch, List<BigInteger> tokenList)
    {
        Preconditions.checkArgument(!tokenList.isEmpty(), "token list must not be empty");
        List<BigInteger> sortedTokens = Ordering.natural().sortedCopy(tokenList);
        int index = Ordering.natural().binarySearch(sortedTokens, tokenToSearch);
        if (index < 0)
        {
            int i = Math.abs(index) - 1;
            if ((i >= sortedTokens.size()) || (i > 0 && sortedTokens.get(i).subtract(tokenToSearch)
                    .compareTo(tokenToSearch.subtract(sortedTokens.get(i - 1))) > 0))
                --i;
            return sortedTokens.get(i);
        }
        return sortedTokens.get(index);
    }

    /**
     * Create an offset to add to token values by hashing the region name.
     */
    public static int regionOffset(String region)
    {
        return Math.abs(region.hashCode());
    }
}
