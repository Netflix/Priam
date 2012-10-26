package com.netflix.priam.utils;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Hex;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ByteOrderedPartitionerTokenManagerTest
{
    @Test(expected = IllegalArgumentException.class)
    public void initialToken_zeroSize()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("0", "ff");
        tokenManager.initialToken(0, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativePosition()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("0", "ff");
        tokenManager.initialToken(1, -1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativeOffset()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("0", "ff");
        tokenManager.initialToken(1, 0, -1);
    }

    @Test
    public void initialToken_positionZero()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("0", "ff");
        assertEquals(toToken("00"), tokenManager.initialToken(1, 0, 0));
        assertEquals(toToken("00"), tokenManager.initialToken(10, 0, 0));
        assertEquals(toToken("00"), tokenManager.initialToken(133, 0, 0));
    }

    @Test
    public void initialToken_offsets_zeroPosition()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager(new CassandraConfiguration());
        assertEquals(toToken("00000000000000000000000000000007"), tokenManager.initialToken(1, 0, 7));
        assertEquals(toToken("0000000000000000000000000000000b"), tokenManager.initialToken(2, 0, 11));
        assertEquals(toToken("0000000000000000000000007fffffff"), tokenManager.initialToken(256, 0, Integer.MAX_VALUE));
    }

    @Test
    public void initialToken_cannotExceedMaximumToken() {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager(new CassandraConfiguration());
        final int maxRingSize = Integer.MAX_VALUE;
        final int maxPosition = maxRingSize - 1;
        final int maxOffset = Integer.MAX_VALUE;
        assertTrue(toToken("ffffffffffffffffffffffffffffffff").compareTo(tokenManager.initialToken(maxRingSize, maxPosition, maxOffset)) > 0);
    }

    @Test
    public void createToken()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager(new CassandraConfiguration());
        assertEquals(Strings.padStart(new BigInteger("ffffffffffffffffffffffffffffffff", 16)
                .add(BigInteger.ONE)
                .divide(BigInteger.valueOf(8 * 32))
                .multiply(BigInteger.TEN)
                .add(BigInteger.valueOf(TokenManager.regionOffset("region")))
                .toString(16), 32, '0'),
                tokenManager.createToken(10, 8, 32, "region"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void findClosestToken_emptyTokenList()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("00", "ff");
        tokenManager.findClosestToken("0", Collections.<String>emptyList());
    }

    @Test
    public void findClosestToken_singleTokenList()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("00", "ff");
        assertEquals("20", tokenManager.findClosestToken("01", ImmutableList.of("20")));
        assertEquals("20", tokenManager.findClosestToken("ff", ImmutableList.of("20")));
    }

    @Test
    public void findClosestToken_multipleTokenList()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("00", "ff");
        List<String> tokenList = ImmutableList.of("0010", "01", "10");
        assertEquals("0010", tokenManager.findClosestToken("0010", tokenList));
        assertEquals("01", tokenManager.findClosestToken("0090", tokenList));
        assertEquals("01", tokenManager.findClosestToken("01", tokenList));
        assertEquals("01", tokenManager.findClosestToken("0120", tokenList));
        assertEquals("01", tokenManager.findClosestToken("0810", tokenList));
        assertEquals("10", tokenManager.findClosestToken("8860", tokenList));
        assertEquals("10", tokenManager.findClosestToken("10", tokenList));
        assertEquals("10", tokenManager.findClosestToken("f0", tokenList));
    }

    @Test
    public void findClosestToken_tieGoesToLargerToken()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("00", "ff");
        assertEquals("a", tokenManager.findClosestToken("5", ImmutableList.of("0", "a")));
    }

    @Test
    public void test4Splits()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager(new CassandraConfiguration());
        final String expectedTokens = "00,40,80,c0";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i]), tokenManager.initialToken(splits, i, 0));
        }
    }

    @Test
    public void test4SplitsWithPrefixSufficientPrecision()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("123456789a", "123456789b");
        final String expectedTokens = "" +
                "123456789a000000000000000000000000000001,123456789a400000000000000000000000000001," +
                "123456789a800000000000000000000000000001,123456789ac00000000000000000000000000001";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i]), tokenManager.initialToken(splits, i, 1));
        }
    }

    @Test
    public void test4SplitsOffset()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager(new CassandraConfiguration());
        final String expectedTokens = "" +
                "00000000000000000000000000000001,40000000000000000000000000000001," +
                "80000000000000000000000000000001,c0000000000000000000000000000001";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i]), tokenManager.initialToken(splits, i, 1));
        }
    }

    @Test
    public void test16Splits()
    {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager(new CassandraConfiguration());
        final String expectedTokens = "00,10,20,30,40,50,60,70,80,90,a0,b0,c0,d0,e0,f0";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i]), tokenManager.initialToken(splits, i, 0));
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
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("00", "ffffffffffffffff");

        int h1 = TokenManager.regionOffset("vijay");
        int h2 = TokenManager.regionOffset("vijay2");
        Token<byte[]> t1 = tokenManager.initialToken(100, 10, h1);
        Token<byte[]> t2 = tokenManager.initialToken(100, 10, h2);

        BigInteger tokenDistance = new BigInteger(1, t1.token).subtract(new BigInteger(1, t2.token));
        long hashDifference = h1 - h2;

        assertEquals(BigInteger.valueOf(hashDifference), tokenDistance);

        Token<byte[]> t3 = tokenManager.initialToken(100, 99, h1);
        Token<byte[]> t4 = tokenManager.initialToken(100, 99, h2);
        tokenDistance = new BigInteger(1, t3.token).subtract(new BigInteger(1, t4.token));

        assertEquals(BigInteger.valueOf(hashDifference), tokenDistance);
    }

    @Test
    public void testNumberToToken() {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("00", "ff");

        assertEquals(toToken("00"), tokenManager.numberToToken(BigInteger.ZERO, 0));
        assertEquals(toToken("00"), tokenManager.numberToToken(BigInteger.ZERO, 1));
        assertEquals(toToken("00"), tokenManager.numberToToken(BigInteger.ZERO, 10));

        assertEquals(toToken("01"), tokenManager.numberToToken(BigInteger.ONE, 1));
        assertEquals(toToken("ff"), tokenManager.numberToToken(BigInteger.valueOf(255), 1));
        assertEquals(toToken("01"), tokenManager.numberToToken(BigInteger.valueOf(256), 2));
        assertEquals(toToken("ff"), tokenManager.numberToToken(BigInteger.valueOf(255 * 256), 2));

        assertEquals(toToken("00000000000000000001"), tokenManager.numberToToken(BigInteger.ONE, 10));

        assertEquals(toToken("0000000000000000ffffffffffffffff"), tokenManager.numberToToken(BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE), 16));
        assertEquals(toToken("ffffffffffffffffffffffffffffffff"), tokenManager.numberToToken(BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE), 16));

        assertEquals(toToken("000000000000000080"), tokenManager.numberToToken(BigInteger.valueOf(2).pow(63), 16));
        assertEquals(toToken("0000000000000001"), tokenManager.numberToToken(BigInteger.valueOf(2).pow(64), 16));
        assertEquals(toToken("80"), tokenManager.numberToToken(BigInteger.valueOf(2).pow(127), 16));
    }

    @Test
    public void testTokenToNumber() {
        ByteOrderedPartitionerTokenManager tokenManager = new ByteOrderedPartitionerTokenManager("00", "ff");

        assertEquals(BigInteger.ZERO, tokenManager.tokenToNumber(toToken("00"), 0));
        assertEquals(BigInteger.ZERO, tokenManager.tokenToNumber(toToken("00"), 1));
        assertEquals(BigInteger.ZERO, tokenManager.tokenToNumber(toToken("00"), 10));

        assertEquals(BigInteger.ONE, tokenManager.tokenToNumber(toToken("01"), 1));
        assertEquals(BigInteger.valueOf(255), tokenManager.tokenToNumber(toToken("ff"), 1));
        assertEquals(BigInteger.valueOf(256), tokenManager.tokenToNumber(toToken("01"), 2));
        assertEquals(BigInteger.valueOf(255*256), tokenManager.tokenToNumber(toToken("ff"), 2));

        assertEquals(BigInteger.ONE, tokenManager.tokenToNumber(toToken("00000000000000000001"), 10));

        assertEquals(BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE), tokenManager.tokenToNumber(toToken("0000000000000000ffffffffffffffff"), 16));
        assertEquals(BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE), tokenManager.tokenToNumber(toToken("ffffffffffffffffffffffffffffffff"), 16));

        assertEquals(BigInteger.valueOf(2).pow(63), tokenManager.tokenToNumber(toToken("000000000000000080"), 16));
        assertEquals(BigInteger.valueOf(2).pow(64), tokenManager.tokenToNumber(toToken("0000000000000001"), 16));
        assertEquals(BigInteger.valueOf(2).pow(127), tokenManager.tokenToNumber(toToken("80"), 16));
    }

    @Test
    public void testLongMinMaxTokens() {
        // First test with 18-byte min/max values, verify the token is created with the expected precision
        ByteOrderedPartitionerTokenManager tokenManager1 = new ByteOrderedPartitionerTokenManager(
                "555500112233445566778899aabbccddeeff",
                "5555ffeeddccbbaa99887766554433221100");
        assertEquals("55552ab616ccd838eefa5b111c7d49764ea4", tokenManager1.createToken(1, 3, 2, "eu-west-1"));

        // Next, test with much longer min/max values.  Verify the extra precision is ignored as not necessary.
        ByteOrderedPartitionerTokenManager tokenManager2 = new ByteOrderedPartitionerTokenManager(
                "555500112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
                "5555ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100");
        assertEquals("55552ab616ccd838eefa5b111c7d49764ea4", tokenManager2.createToken(1, 3, 2, "eu-west-1"));
    }

    private static BytesToken toToken(String string) {
        return new BytesToken(Hex.hexToBytes(string));
    }
}
