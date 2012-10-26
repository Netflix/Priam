package com.netflix.priam.identity;

import java.util.List;

/**
 * Interface to manage membership meta information such as size of RAC, list of
 * nodes in RAC etc. Also perform ACL updates used in multi-regional clusters
 */
public interface IMembership {
    /**
     * Get a list of Instances in the current Auto Scale Group
     *
     * @return
     */
    public List<String> getAutoScaleGroupMembership();

    /**
     * @return Size of current RAC
     */
    public int getAvailabilityZoneMembershipSize();

    /**
     * Number of Availability Zones
     *
     * @return
     */
    public int getUsableAvailabilityZones();

    /**
     * Expand the membership size by 1.
     *
     * @param count
     */
    public void expandAvailabilityZoneMembership(int count);
}