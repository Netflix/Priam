package com.netflix.priam.utils;

import java.math.BigInteger;
import java.util.List;

public interface ITokenManager
{
    String createToken(int mySlot, int racCount, int racSize, String region);

    String createToken(int mySlot, int totalCount, String region);

    BigInteger findClosestToken(BigInteger tokenToSearch, List<BigInteger> tokenList);

    int regionOffset(String region);
}
