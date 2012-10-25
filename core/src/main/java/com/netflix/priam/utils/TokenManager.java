package com.netflix.priam.utils;

import java.util.List;

public abstract class TokenManager {
    /**
     * Creates a token given the following parameter
     *
     * @param mySlot   -- Slot where this instance has to be.
     * @param availabilityZones -- The number of AvailabilityZones
     * @param availabilityZoneMembershipSize  -- number of members in the availabilityZone
     * @param region    -- name of the DC where it this token is created.
     */
    public String createToken(int mySlot, int availabilityZones, int availabilityZoneMembershipSize, String region) {
        return createToken(mySlot, availabilityZones * availabilityZoneMembershipSize, region);
    }

    public abstract String createToken(int mySlot, int totalCount, String region);

    public abstract String findClosestToken(String tokenToSearch, List<String> tokenList);

    /**
     * Create an offset to add to token values by hashing the region name.
     */
    public static int regionOffset(String region) {
        return Math.abs(region.hashCode());
    }
}
