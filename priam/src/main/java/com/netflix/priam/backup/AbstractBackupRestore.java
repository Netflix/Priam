/*
 * Copyright 2016 Netflix, Inc.
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

package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by aagrawal on 8/14/17.
 */
public abstract class AbstractBackupRestore extends Task {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBackupRestore.class);
    public static String JOBNAME = "AbstractBackupRestore";

    protected IConfiguration config;
    protected final Map<String, List<String>> columnFamilyFilter = new HashMap<>(); //key: keyspace, value: a list of CFs within the keyspace
    protected final Map<String, Object> keyspaceFilter = new HashMap<>(); //key: keyspace, value: null

    @Inject
    public AbstractBackupRestore(IConfiguration config)
    {
        super(config);
        this.config = config;
    }

    /*
 * search for "1:* alphanumeric chars including special chars""literal period"" 1:* alphanumeric chars  including special chars"
 * @param input string
 * @return true if input string matches search pattern; otherwise, false
 */
    protected final boolean isValidCFFilterFormat(String cfFilter) {
        Pattern p = Pattern.compile(".\\..");
        Matcher m = p.matcher(cfFilter);
        return m.find();
    }

    protected abstract String getConfigKeyspaceFilter();

    protected abstract String getConfigColumnfamilyFilter();

    protected final void populateFilters() {
        String configKeyspaceFilter = getConfigKeyspaceFilter();
        if (configKeyspaceFilter == null || configKeyspaceFilter.isEmpty()) {
            logger.info(String.format("No keyspace filter set for {}.", JOBNAME));
        } else {
            String[] keyspaces = configKeyspaceFilter.split(",");
            for (int i = 0; i < keyspaces.length; i++) {
                logger.info(String.format("Adding {} keyspace filter: {}", JOBNAME, keyspaces[i]));
                this.keyspaceFilter.put(keyspaces[i], null);
            }

        }

        String configColumnfamilyFilter = getConfigColumnfamilyFilter();
        if (configColumnfamilyFilter == null || configColumnfamilyFilter.isEmpty()) {

            logger.info(String.format("No column family filter set for {}.", JOBNAME));

        } else {

            String[] filters = configColumnfamilyFilter.split(",");
            for (int i = 0; i < filters.length; i++) { //process each filter
                if (isValidCFFilterFormat(filters[i])) {

                    String[] filter = filters[i].split("\\.");
                    String ksName = filter[0];
                    String cfName = filter[1];
                    logger.info(String.format("Adding {} CF filter: {}.{}", JOBNAME, ksName, cfName));

                    if (this.columnFamilyFilter.containsKey(ksName)) {
                        //add cf to existing filter
                        List<String> cfs = this.columnFamilyFilter.get(ksName);
                        cfs.add(cfName);
                        this.columnFamilyFilter.put(ksName, cfs);

                    } else {

                        List<String> cfs = new ArrayList<String>();
                        cfs.add(cfName);
                        this.columnFamilyFilter.put(ksName, cfs);

                    }

                } else {
                    throw new IllegalArgumentException("Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: " + filters[i]);
                }
            } //end processing each filter

        }
    }

    /*
   * @param keyspace or columnfamily directory type.
   * @return true if directory should be filter from processing; otherwise, false.
   */
    protected final boolean isFiltered(DIRECTORYTYPE directoryType, String... args) {

        if (directoryType.equals(DIRECTORYTYPE.KEYSPACE)) { //start with filtering the parent (keyspace)
            //Apply each keyspace filter to input string
            String keyspaceName = args[0];

            java.util.Set<String> ksFilters = keyspaceFilter.keySet();
            Iterator<String> it = ksFilters.iterator();
            while (it.hasNext()) {
                String ksFilter = it.next();
                Pattern p = Pattern.compile(ksFilter);
                Matcher m = p.matcher(keyspaceName);
                if (m.find()) {
                    logger.info("Keyspace: " + keyspaceName + " matched filter: " + ksFilter);
                    return true;
                }
            }

        }

        if (directoryType.equals(DIRECTORYTYPE.CF)) { //parent (keyspace) is not filtered, now see if the child (CF) is filtered
            String keyspaceName = args[0];
            if (!columnFamilyFilter.containsKey(keyspaceName)) {
                return false;
            }

            String cfName = args[1];
            List<String> cfsFilter = columnFamilyFilter.get(keyspaceName);
            for (int i = 0; i < cfsFilter.size(); i++) {
                Pattern p = Pattern.compile(cfsFilter.get(i));
                Matcher m = p.matcher(cfName);
                if (m.find()) {
                    logger.info(keyspaceName + "." + cfName + " matched filter");
                    return true;
                }
            }
        }

        return false; //if here, current input are not part of keyspae and cf filters
    }

    public enum DIRECTORYTYPE {
        KEYSPACE, CF
    }
}
