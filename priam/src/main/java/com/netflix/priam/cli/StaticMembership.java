/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.cli;

import com.google.common.collect.ImmutableSet;
import com.netflix.priam.identity.IMembership;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticMembership implements IMembership {
    private static final String MEMBERSHIP_PRE = "membership.";
    private static final String INSTANCES_PRE = MEMBERSHIP_PRE + "instances.";
    private static final String RAC_NAME = MEMBERSHIP_PRE + "racname";

    private static final String DEFAULT_PROP_PATH = "/etc/priam/membership.properties";

    private static final Logger logger = LoggerFactory.getLogger(StaticMembership.class);

    private ImmutableSet<String> racMembership;
    private int racCount;

    public StaticMembership() throws IOException {
        Properties config = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(DEFAULT_PROP_PATH);
            config.load(fis);
        } catch (Exception e) {
            logger.error("Exception with static membership file ", e);
            throw new RuntimeException("Problem reading static membership file. Cannot start.", e);
        } finally {
            FileUtils.closeQuietly(fis);
        }
        String racName = config.getProperty(RAC_NAME);
        racCount = 0;
        for (String name : config.stringPropertyNames()) {
            if (name.startsWith(INSTANCES_PRE)) {
                racCount += 1;
                if (name.equals(INSTANCES_PRE + racName))
                    racMembership = ImmutableSet.copyOf(config.getProperty(name).split(","));
            }
        }
    }

    @Override
    public ImmutableSet<String> getRacMembership() {
        return racMembership;
    }

    @Override
    public ImmutableSet<String> getCrossAccountRacMembership() {
        return null;
    }

    @Override
    public int getRacMembershipSize() {
        if (racMembership == null) return 0;
        return racMembership.size();
    }

    @Override
    public int getRacCount() {
        return racCount;
    }

    @Override
    public void addACL(Collection<String> listIPs, int from, int to) {}

    @Override
    public void removeACL(Collection<String> listIPs, int from, int to) {}

    @Override
    public ImmutableSet<String> listACL(int from, int to) {
        return null;
    }

    @Override
    public void expandRacMembership(int count) {}
}
