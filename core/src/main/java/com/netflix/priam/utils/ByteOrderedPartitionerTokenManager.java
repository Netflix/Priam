package com.netflix.priam.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Objects;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Token;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ByteOrderedPartitionerTokenManager extends TokenManager {
    // The most minimum token is the empty byte array, but that look like blank token when stringified and can be
    // confused with an unspecified token.  For our token calculations the min & max don't have to be exact, just
    // close enough to pick balanced ranges.  So use "00" as a less confusing minimum token.
    private static final String DEFAULT_MINIMUM_TOKEN = "00";
    private static final String DEFAULT_MAXIMUM_TOKEN = "ffffffffffffffffffffffffffffffff";

    // Tokens are expected to be lowercase hex.  The findClosestToken method will break if uppercase hex.
    private static final CharMatcher VALID_TOKEN = CharMatcher.inRange('0', '9').or(CharMatcher.inRange('a', 'f'));

    private final ByteOrderedPartitioner partitioner = new ByteOrderedPartitioner();
    private final Token<byte[]> minimumToken;
    private final Token<byte[]> maximumToken;

    @Inject
    public ByteOrderedPartitionerTokenManager(CassandraConfiguration config) {
        this(Objects.firstNonNull(config.getMinimumToken(), DEFAULT_MINIMUM_TOKEN),
                Objects.firstNonNull(config.getMaximumToken(), DEFAULT_MAXIMUM_TOKEN));
    }

    @VisibleForTesting
    ByteOrderedPartitionerTokenManager(String minimumToken, String maximumToken) {
        this.minimumToken = partitioner.getTokenFactory().fromString(minimumToken);
        this.maximumToken = partitioner.getTokenFactory().fromString(maximumToken);
        checkArgument(this.minimumToken.compareTo(this.maximumToken) < 0,
                "Minimum token must be < maximum token: %s %s", minimumToken, maximumToken);
    }

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
    Token<byte[]> initialToken(int size, int position, int offset) {
        checkArgument(size > 0, "size must be > 0");
        checkArgument(offset >= 0, "offset must be >= 0");
        checkArgument(position >= 0, "position must be >= 0");

        // Assume keys are distributed evenly between the minimum and maximum token.  This is often a bad assumption
        // with the ByteOrderedPartitioner, but that's why everyone is discouraged from using it.

        // Subdivide between the min and max using roughly 16 bytes of precision, same size as RandomPartitioner tokens.
        int tokenLength = getIndexOfFirstDifference(minimumToken.token, maximumToken.token) + 16;
        BigInteger min = tokenToNumber(minimumToken, tokenLength);
        BigInteger max = tokenToNumber(maximumToken, tokenLength);

        BigInteger value = max.add(BigInteger.ONE)  // add 1 since max is inclusive, helps get the splits to round #s
                .subtract(min)
                .divide(BigInteger.valueOf(size))
                .multiply(BigInteger.valueOf(position))
                .add(BigInteger.valueOf(offset))
                .add(min);
        Token<byte[]> token = numberToToken(value, tokenLength);

        // Make sure the token stays within the configured bounds.
        return Ordering.natural().min(Ordering.natural().max(token, minimumToken), maximumToken);
    }

    @Override
    public String createToken(int mySlot, int totalCount, String region) {
        return partitioner.getTokenFactory().toString(initialToken(totalCount, mySlot, regionOffset(region)));
    }

    @Override
    public String findClosestToken(String tokenToSearch, List<String> tokenList) {
        checkArgument(!tokenList.isEmpty(), "token list must not be empty");
        checkArgument(VALID_TOKEN.matchesAllOf(tokenToSearch), "token must be lowercase hex: %s", tokenToSearch);
        for (String token : tokenList) {
            checkArgument(VALID_TOKEN.matchesAllOf(token), "token must be lowercase hex: %s", token);
        }

        // Rely on the fact that hex-encoded strings sort in the same relative order as the BytesToken byte arrays.
        List<String> sortedTokens = Ordering.natural().sortedCopy(tokenList);
        int index = Ordering.natural().binarySearch(sortedTokens, tokenToSearch);
        if (index < 0) {
            int i = -index - 1;
            if ((i >= sortedTokens.size()) ||
                    (i > 0 && lessThanMidPoint(sortedTokens.get(i - 1), tokenToSearch, sortedTokens.get(i)))) {
                --i;
            }
            return sortedTokens.get(i);
        }
        return sortedTokens.get(index);
    }

    private boolean lessThanMidPoint(String min, String token, String max) {
        Token.TokenFactory<byte[]> tf = partitioner.getTokenFactory();
        BytesToken midpoint = partitioner.midpoint(tf.fromString(min), tf.fromString(max));
        return tf.fromString(token).compareTo(midpoint) < 0;
    }

    @VisibleForTesting
    BigInteger tokenToNumber(Token<byte[]> token, int tokenLength) {
        // Right-pad with zeros up to the token length
        return new BigInteger(1, Arrays.copyOf(token.token, tokenLength));
    }

    @VisibleForTesting
    Token<byte[]> numberToToken(BigInteger number, int tokenLength) {
        checkArgument(number.signum() >= 0, "Token math should not yield negative numbers: %s", number);
        byte[] numberBytes = number.toByteArray();
        int numberOffset = numberBytes[0] != 0 ? 0 : 1;  // Ignore the first if it's zero ("sign byte") to ensure byte[] length <= tokenLength.
        int numberLength = numberBytes.length - numberOffset;
        checkArgument(numberLength <= tokenLength, "Token math should not yield tokens bigger than maxToken (%s bytes): %s", tokenLength, number);

        // Trim trailing zeros that we likely added in tokenToNumber() when right-padding the number up to the token length.
        while (numberLength > 0 && numberBytes[numberOffset + numberLength - 1] == 0) {
            numberLength--;
            tokenLength--;
        }
        // Special case for number==0.  Trim down to a byte array of length 1.
        if (numberLength == 0) {
            tokenLength = 1;
        }

        // Left-pad the number with zeros when creating the number array.
        byte[] tokenBytes = new byte[tokenLength];
        System.arraycopy(numberBytes, numberOffset, tokenBytes, tokenBytes.length - numberLength, numberLength);
        return partitioner.getToken(ByteBuffer.wrap(tokenBytes));
    }

    /**
     * Returns the index of the first byte following the prefix common to A and B.
     */
    private int getIndexOfFirstDifference(byte[] tokenA, byte[] tokenB) {
        int length = Math.min(tokenA.length, tokenB.length);
        for (int i = 0; i < length; i++) {
            if (tokenA[i] != tokenB[i]) {
                return i;
            }
        }
        return length;
    }
}
