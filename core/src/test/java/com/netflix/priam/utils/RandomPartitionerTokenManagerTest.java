package com.netflix.priam.utils;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.dht.RandomPartitioner;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RandomPartitionerTokenManagerTest
{
    private static final BigInteger MINIMUM_TOKEN = BigInteger.ZERO;
    private static final BigInteger MAXIMUM_TOKEN = RandomPartitioner.MAXIMUM;

    private final RandomPartitionerTokenManager tokenManager = new RandomPartitionerTokenManager();

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_zeroSize()
    {
        tokenManager.initialToken(0, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativePosition()
    {
        tokenManager.initialToken(1, -1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativeOffset()
    {
        tokenManager.initialToken(1, 0, -1);
    }

    @Test
    public void initialToken_positionZero()
    {
        assertEquals(MINIMUM_TOKEN, tokenManager.initialToken(1, 0, 0));
        assertEquals(MINIMUM_TOKEN, tokenManager.initialToken(10, 0, 0));
        assertEquals(MINIMUM_TOKEN, tokenManager.initialToken(133, 0, 0));
    }

    @Test
    public void initialToken_offsets_zeroPosition()
    {
        assertEquals(MINIMUM_TOKEN.add(BigInteger.valueOf(7)), tokenManager.initialToken(1, 0, 7));
        assertEquals(MINIMUM_TOKEN.add(BigInteger.valueOf(11)), tokenManager.initialToken(2, 0, 11));
        assertEquals(MINIMUM_TOKEN.add(BigInteger.valueOf(Integer.MAX_VALUE)),
                tokenManager.initialToken(256, 0, Integer.MAX_VALUE));
    }
    
    @Test
    public void initialToken_cannotExceedMaximumToken() {
        final int maxRingSize = Integer.MAX_VALUE;
        final int maxPosition = maxRingSize - 1;
        final int maxOffset = Integer.MAX_VALUE;
        assertTrue(MAXIMUM_TOKEN.compareTo(tokenManager.initialToken(maxRingSize, maxPosition, maxOffset)) > 0);
    }

    @Test
    public void createToken()
    {
        assertEquals(MAXIMUM_TOKEN.divide(BigInteger.valueOf(8 * 32))
                .multiply(BigInteger.TEN)
                .add(BigInteger.valueOf(TokenManager.regionOffset("region")))
                .toString(),
                tokenManager.createToken(10, 8, 32, "region"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void findClosestToken_emptyTokenList()
    {
        tokenManager.findClosestToken("0", Collections.<String>emptyList());
    }

    @Test
    public void findClosestToken_singleTokenList()
    {
        assertEquals("100", tokenManager.findClosestToken("10", ImmutableList.of("100")));
    }

    @Test
    public void findClosestToken_multipleTokenList()
    {
        List<String> tokenList = ImmutableList.of("1", "10", "100");
        assertEquals("1", tokenManager.findClosestToken("1", tokenList));
        assertEquals("10", tokenManager.findClosestToken("9", tokenList));
        assertEquals("10", tokenManager.findClosestToken("10", tokenList));
        assertEquals("10", tokenManager.findClosestToken("12", tokenList));
        assertEquals("10", tokenManager.findClosestToken("51", tokenList));
        assertEquals("100", tokenManager.findClosestToken("56", tokenList));
        assertEquals("100", tokenManager.findClosestToken("100", tokenList));
    }

    @Test
    public void findClosestToken_tieGoesToLargerToken()
    {
        assertEquals("10", tokenManager.findClosestToken("5", ImmutableList.of("0", "10")));
    }

    @Test
    public void test4Splits()
    {
        // example tokens from http://wiki.apache.org/cassandra/Operations
        final String expectedTokens = "0,42535295865117307932921825928971026432,"
                + "85070591730234615865843651857942052864,127605887595351923798765477786913079296";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(new BigInteger(tokens[i]), tokenManager.initialToken(splits, i, 0));
        }
    }

    @Test
    public void test16Splits()
    {
        final String expectedTokens = "0,10633823966279326983230456482242756608,"
                + "21267647932558653966460912964485513216,31901471898837980949691369446728269824,"
                + "42535295865117307932921825928971026432,53169119831396634916152282411213783040,"
                + "63802943797675961899382738893456539648,74436767763955288882613195375699296256,"
                + "85070591730234615865843651857942052864,95704415696513942849074108340184809472,"
                + "106338239662793269832304564822427566080,116972063629072596815535021304670322688,"
                + "127605887595351923798765477786913079296,138239711561631250781995934269155835904,"
                + "148873535527910577765226390751398592512,159507359494189904748456847233641349120";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(new BigInteger(tokens[i]), tokenManager.initialToken(splits, i, 0));
        }
    }

    @Test
    public void regionOffset()
    {
        String allRegions = "us-west-2,us-east,us-west,eu-east,eu-west,ap-northeast,ap-southeast";

        for (String region1 : allRegions.split(",")) {
            for (String region2 : allRegions.split(",")) {
                if (region1.equals(region2)) {
                    continue;
                }
                assertFalse("Difference seems to be low",
                        Math.abs(TokenManager.regionOffset(region1) - TokenManager.regionOffset(region2)) < 100);
            }
        }
    }

    @Test
    public void testMultiToken()
    {
        int h1 = TokenManager.regionOffset("vijay");
        int h2 = TokenManager.regionOffset("vijay2");
        BigInteger t1 = tokenManager.initialToken(100, 10, h1);
        BigInteger t2 = tokenManager.initialToken(100, 10, h2);

        BigInteger tokenDistance = t1.subtract(t2).abs();
        int hashDifference = h1 - h2;

        assertEquals(new BigInteger("" + hashDifference).abs(), tokenDistance);

        BigInteger t3 = tokenManager.initialToken(100, 99, h1);
        BigInteger t4 = tokenManager.initialToken(100, 99, h2);
        tokenDistance = t3.subtract(t4).abs();

        assertEquals(new BigInteger("" + hashDifference).abs(), tokenDistance);
    }
}
