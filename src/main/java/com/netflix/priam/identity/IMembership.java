package com.netflix.priam.identity;

import java.util.Collection;
import java.util.List;

/**
 * Interface to manage membership meta information such as size of RAC, list of
 * nodes in RAC etc. Also perform ACL updates used in multi-regional clusters
 */
public interface IMembership {
    /**
     * Get a list of Instances in the current RAC
     *
     * @return
     */
    public List<String> getRacMembership();

    /**
     * @return Size of current RAC
     */
    public int getAvailabilityZoneMembershipSize();

    /**
     * Number of RACs
     *
     * @return
     */
    public int getRacCount();

    /**
     * Add security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    public void addACL(Collection<String> listIPs, int from, int to);

    /**
     * Remove security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    public void removeACL(Collection<String> listIPs, int from, int to);

    /**
     * List all ACLs
     */
    public List<String> listACL(int from, int to);

    /**
     * Expand the membership size by 1.
     *
     * @param count
     */
    public void expandAvailabilityZoneMembership(int count);
}