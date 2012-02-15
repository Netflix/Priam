package com.netflix.priam.cassandra.token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigInteger;

import org.junit.Test;

import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TokenManager;

public class TokenManagementTest
{

    int splits = 16;
    String sampleToken = "0,10633823966279326983230456482242756607," + "21267647932558653966460912964485513215,31901471898837980949691369446728269823,"
            + "42535295865117307932921825928971026431,53169119831396634916152282411213783039," + "63802943797675961899382738893456539647,74436767763955288882613195375699296255,"
            + "85070591730234615865843651857942052863,95704415696513942849074108340184809471," + "106338239662793269832304564822427566079,116972063629072596815535021304670322687,"
            + "127605887595351923798765477786913079295,138239711561631250781995934269155835903," + "148873535527910577765226390751398592511,159507359494189904748456847233641349119";

    @Test
    public void testSplit()
    {
        String[] tokens = sampleToken.split(",");
        for (int i = 0; i < splits; i++)
            assertEquals(TokenManager.intialToken(splits, i, 0), new BigInteger(tokens[i]));
    }

    @Test
    public void printSplit()
    {
        for (int i = 0; i < 18; i++)
            System.out.println(TokenManager.intialToken(18, i, 1808575600));
    }

    @Test
    public void testCustomHash()
    {
        System.out.println("");

        String allRegions = "us-west-2,us-east,us-west,eu-east,eu-west,ap-northeast,ap-southeast";

        for (String region1 : allRegions.split(","))
            for (String region2 : allRegions.split(","))
            {
                if (region1.equals(region2))
                    continue;
                assertFalse("Diffrence seems to be low", Math.abs(SystemUtils.hash(region1) - SystemUtils.hash(region2)) < 100);
            }
    }

    @Test
    public void testMultiToken()
    {
        int h1 = SystemUtils.hash("vijay");
        int h2 = SystemUtils.hash("vijay2");
        BigInteger t1 = TokenManager.intialToken(100, 10, h1);
        BigInteger t2 = TokenManager.intialToken(100, 10, h2);

        BigInteger tokendistance = t1.subtract(t2).abs();
        int hashDiffrence = h1 - h2;

        assert (new BigInteger("" + hashDiffrence).abs().equals(tokendistance));

        BigInteger t3 = TokenManager.intialToken(100, 99, h1);
        BigInteger t4 = TokenManager.intialToken(100, 99, h2);
        tokendistance = t3.subtract(t4).abs();

        assert (new BigInteger("" + hashDiffrence).abs().equals(tokendistance));
    }
}
