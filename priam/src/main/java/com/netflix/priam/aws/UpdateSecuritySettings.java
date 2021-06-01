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
package com.netflix.priam.aws;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * this class will associate an Public IP's with a new instance so they can talk across the regions.
 *
 * <p>Requirement: 1) Nodes in the same region needs to be able to talk to each other. 2) Nodes in
 * other regions needs to be able to talk to t`he others in the other region.
 *
 * <p>Assumption: 1) IPriamInstanceFactory will provide the membership... and will be visible across
 * the regions 2) IMembership amazon or any other implementation which can tell if the instance is
 * part of the group (ASG in amazons case).
 */
@Singleton
public class UpdateSecuritySettings extends Task {
    private static final Logger logger = LoggerFactory.getLogger(UpdateSecuritySettings.class);
    public static final String JOBNAME = "Update_SG";
    public static boolean firstTimeUpdated = false;

    private static final Random ran = new Random();
    private final IMembership membership;
    private final IPriamInstanceFactory factory;
    private final IPConverter ipConverter;

    @Inject
    // Note: do not parameterized the generic type variable to an implementation as it confuses
    // Guice in the binding.
    public UpdateSecuritySettings(
            IConfiguration config,
            IMembership membership,
            IPriamInstanceFactory factory,
            IPConverter ipConverter) {
        super(config);
        this.membership = membership;
        this.factory = factory;
        this.ipConverter = ipConverter;
    }

    /**
     * Seeds nodes execute this at the specifed interval. Other nodes run only on startup. Seeds in
     * cassandra are the first node in each Availablity Zone.
     */
    @Override
    public void execute() {
        int port = config.getSSLStoragePort();
        ImmutableSet<String> currentAcl = membership.listACL(port, port);
        Set<String> desiredAcl =
                factory.getAllIds(config.getAppName())
                        .stream()
                        .map(i -> getIngressRule(i).orElse(null))
                        .filter(Objects::nonNull)
                        .map(ip -> ip + "/32")
                        .collect(Collectors.toSet());
        if (!config.skipDeletingOthersIngressRules()) {
            Set<String> aclToRemove = Sets.difference(currentAcl, desiredAcl);
            logger.info("ingress rules to delete: {}", Joiner.on(",").join(aclToRemove));
            if (!aclToRemove.isEmpty()) {
                membership.removeACL(aclToRemove, port, port);
                firstTimeUpdated = true;
            }
        }
        if (!config.skipUpdatingOthersIngressRules()) {
            Set<String> aclToAdd = Sets.difference(desiredAcl, currentAcl);
            logger.info("ingress rules to update: {}", Joiner.on(",").join(aclToAdd));
            if (!aclToAdd.isEmpty()) {
                int resultingSize = aclToAdd.size() + currentAcl.size();
                if (resultingSize > config.getACLSizeWarnThreshold()) {
                    logger.warn("We are trying to make too many ingress rules! {}", resultingSize);
                }
                membership.addACL(aclToAdd, port, port);
                firstTimeUpdated = true;
            }
        }
    }

    private Optional<String> getIngressRule(PriamInstance instance) {
        return config.skipIngressUnlessIPIsPublic()
                ? ipConverter.getPublicIP(instance)
                : Optional.of(instance.getHostIP());
    }

    public static TaskTimer getTimer(InstanceIdentity id) {
        SimpleTimer return_;
        if (id.isSeed()) {
            logger.info(
                    "Seed node.  Instance id: {}" + ", host ip: {}" + ", host name: {}",
                    id.getInstance().getInstanceId(),
                    id.getInstance().getHostIP(),
                    id.getInstance().getHostName());
            return_ = new SimpleTimer(JOBNAME, 120 * 1000 + ran.nextInt(120 * 1000));
        } else return_ = new SimpleTimer(JOBNAME);
        return return_;
    }

    @Override
    public String getName() {
        return JOBNAME;
    }
}
