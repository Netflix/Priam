package com.priam.utils;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenManager
{
    private static final Logger logger = LoggerFactory.getLogger(TokenManager.class);
    protected static final BigInteger MINIMUM_TOKEN = new BigInteger("0");
    protected static final BigInteger MAXIMUM_TOKEN = new BigInteger("2").pow(127);

    public static BigInteger intialToken(int size, int position, int space)
    {
        BigInteger decValue = MINIMUM_TOKEN;
        // (Maximum * max_slots * my_slot) -1
        if (position != 0)
            decValue = MAXIMUM_TOKEN.divide(new BigInteger("" + size)).multiply(new BigInteger("" + position)).subtract(new BigInteger("" + 1));
        // Add a Region/DC spacer to the token.
        decValue = decValue.add(new BigInteger("" + space));
        // if the space is bigger then rotate to min for the ring.
        if (1 == decValue.compareTo(MAXIMUM_TOKEN))
        {
            BigInteger delta = decValue.subtract(MAXIMUM_TOKEN);
            decValue = MINIMUM_TOKEN.add(delta);
        }
        //logger.debug(String.format("Computed token for the slot %d is %s with a region spacer as %d:", position, decValue.toString(), space));
        // return the computed value.
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
        int space = SystemUtils.hash(region);
        int regionCount = rac_count * rac_size;
        return intialToken(regionCount, my_slot, space).toString();
    }
    
    public static String createToken(int my_slot, int totalCount, String region)
    {
        int space = SystemUtils.hash(region);
        return intialToken(totalCount, my_slot, space).toString();
    }
    
    public static BigInteger findClosestToken(BigInteger tokenToSearch, List<BigInteger> tokenList)
    {
        Collections.sort(tokenList);
        int index = Collections.binarySearch(tokenList, tokenToSearch);
        return tokenList.get(Math.abs(index));
    }
}
