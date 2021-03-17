/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.identity;

import com.google.common.collect.ImmutableSet;
import com.google.inject.ImplementedBy;
import com.netflix.priam.aws.AWSMembership;
import java.util.Collection;

/**
 * Interface to manage membership meta information such as size of RAC, list of nodes in RAC etc.
 * Also perform ACL updates used in multi-regional clusters
 */
@ImplementedBy(AWSMembership.class)
public interface IMembership {
    /**
     * Get a list of Instances in the current RAC
     *
     * @return
     */
    ImmutableSet<String> getRacMembership();

    /** @return Size of current RAC */
    int getRacMembershipSize();

    /**
     * Get a set of Instances in the cross-account but current RAC
     *
     * @return
     */
    ImmutableSet<String> getCrossAccountRacMembership();

    /**
     * Number of RACs
     *
     * @return
     */
    int getRacCount();

    /**
     * Add security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    void addACL(Collection<String> listIPs, int from, int to);

    /**
     * Remove security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    void removeACL(Collection<String> listIPs, int from, int to);

    /**
     * List all ACLs
     *
     * @return
     */
    ImmutableSet<String> listACL(int from, int to);

    /**
     * Expand the membership size by 1.
     *
     * @param count
     */
    void expandRacMembership(int count);
}
