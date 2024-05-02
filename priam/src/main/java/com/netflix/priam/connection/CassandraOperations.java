/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.priam.connection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.netflix.priam.backup.BackupRestoreUtil;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.CassandraMonitor;
import com.netflix.priam.utils.RetryableCallable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import javax.inject.Inject;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class encapsulates interactions with Cassandra. Created by aagrawal on 6/19/18. */
public class CassandraOperations implements ICassandraOperations {
    private static final Logger logger = LoggerFactory.getLogger(CassandraOperations.class);
    private final IConfiguration configuration;

    @Inject
    public CassandraOperations(IConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public synchronized void takeSnapshot(final String snapshotName) throws Exception {
        // Retry max of 6 times with 10 second in between (for one minute). This is to ensure that
        // we overcome any temporary glitch.
        // Note that operation MAY fail if cassandra successfully took the snapshot of certain
        // columnfamily(ies) and we try to create snapshot with
        // same name. It is a good practice to call clearSnapshot after this operation fails, to
        // ensure we don't leave any left overs.
        // Example scenario: Change of file permissions by manual intervention and C* unable to take
        // snapshot of one CF.
        try {
            new RetryableCallable<Void>(6, 10000) {
                public Void retriableCall() throws Exception {
                    JMXNodeTool nodetool = JMXNodeTool.instance(configuration);
                    nodetool.takeSnapshot(snapshotName, null, Collections.emptyMap());
                    return null;
                }
            }.call();
        } catch (Exception e) {
            logger.error(
                    "Error while taking snapshot {}. Asking Cassandra to clear snapshot to avoid accumulation of snapshots.",
                    snapshotName);
            clearSnapshot(snapshotName);
            throw e;
        }
    }

    @Override
    public void clearSnapshot(final String snapshotTag) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(configuration);
                nodetool.clearSnapshot(snapshotTag);
                return null;
            }
        }.call();
    }

    @Override
    public List<String> getKeyspaces() throws Exception {
        return new RetryableCallable<List<String>>() {
            public List<String> retriableCall() throws Exception {
                try (JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                    return nodeTool.getKeyspaces();
                }
            }
        }.call();
    }

    @Override
    public Map<String, List<String>> getColumnfamilies() throws Exception {
        return new RetryableCallable<Map<String, List<String>>>() {
            public Map<String, List<String>> retriableCall() throws Exception {
                try (JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                    final Map<String, List<String>> columnfamilies = new HashMap<>();
                    Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> columnfamilyStoreMBean =
                            nodeTool.getColumnFamilyStoreMBeanProxies();
                    columnfamilyStoreMBean.forEachRemaining(
                            entry -> {
                                columnfamilies.putIfAbsent(entry.getKey(), new ArrayList<>());
                                columnfamilies
                                        .get(entry.getKey())
                                        .add(entry.getValue().getColumnFamilyName());
                            });
                    return columnfamilies;
                }
            }
        }.call();
    }

    @Override
    public void forceKeyspaceCompaction(String keyspaceName, String... columnfamilies)
            throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                try (JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                    nodeTool.forceKeyspaceCompaction(false, keyspaceName, columnfamilies);
                    return null;
                }
            }
        }.call();
    }

    @Override
    public void forceKeyspaceFlush(String keyspaceName) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                try (JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                    nodeTool.forceKeyspaceFlush(keyspaceName);
                    return null;
                }
            }
        }.call();
    }

    @Override
    public List<Map<String, String>> gossipInfo() throws Exception {
        List<Map<String, String>> returnPublicIpSourceIpMap = new ArrayList();
        try {
            JMXNodeTool nodeTool;
            try {
                nodeTool = JMXNodeTool.instance(configuration);
            } catch (JMXConnectionException e) {
                logger.error(
                        "Exception in fetching c* jmx tool .  Msg: {}", e.getLocalizedMessage(), e);
                throw e;
            }
            String gossipInfoLines[] = nodeTool.getGossipInfo(false).split("/");
            Arrays.stream(gossipInfoLines)
                    .forEach(
                            gossipInfoLine -> {
                                Map<String, String> gossipMap = new HashMap<>();
                                String gossipInfoSubLines[] = gossipInfoLine.split("\\r?\\n");
                                if (gossipInfoSubLines.length
                                        > 2) // Random check for existence of some lines
                                {
                                    gossipMap.put("PUBLIC_IP", gossipInfoSubLines[0].trim());
                                    if (gossipMap.get("PUBLIC_IP") != null) {
                                        returnPublicIpSourceIpMap.add(gossipMap);
                                    }

                                    for (String gossipInfoSubLine : gossipInfoSubLines) {
                                        String gossipLineEntry[] = gossipInfoSubLine.split(":");
                                        if (gossipLineEntry.length == 2) {
                                            gossipMap.put(
                                                    gossipLineEntry[0].trim().toUpperCase(),
                                                    gossipLineEntry[1].trim());
                                        } else if (gossipLineEntry.length == 3) {
                                            if (gossipLineEntry[0]
                                                    .trim()
                                                    .equalsIgnoreCase("STATUS")) {
                                                // Special handling for STATUS as C* puts first
                                                // token in STATUS or "true".
                                                gossipMap.put(
                                                        gossipLineEntry[0].trim().toUpperCase(),
                                                        gossipLineEntry[2].split(",")[0].trim());
                                            } else if (gossipLineEntry[0]
                                                    .trim()
                                                    .equalsIgnoreCase("TOKENS")) {
                                                // Special handling for tokens as it is always
                                                // "hidden".
                                                try {
                                                    gossipMap.put(
                                                            gossipLineEntry[0].trim().toUpperCase(),
                                                            nodeTool.getTokens(
                                                                            gossipMap.get(
                                                                                    "PUBLIC_IP"))
                                                                    .toString());
                                                } catch (Exception e) {
                                                    logger.warn(
                                                            "Unable to find TOKEN(s) for the IP: {}",
                                                            gossipMap.get("PUBLIC_IP"));
                                                }
                                            } else {
                                                gossipMap.put(
                                                        gossipLineEntry[0].trim().toUpperCase(),
                                                        gossipLineEntry[2].trim());
                                            }
                                        }
                                    }
                                }
                            });

        } catch (Exception e) {
            logger.error("Unable to parse nodetool gossipinfo output from Cassandra.", e);
        }
        return returnPublicIpSourceIpMap;
    }

    @Override
    public List<String> importAll(String srcDir) throws IOException {
        List<String> failedImports = new ArrayList<>();
        if (CassandraMonitor.hasCassadraStarted()) {
            for (Path tableDir : BackupRestoreUtil.getBackupDirectories(srcDir, "")) {
                String keyspace = tableDir.getParent().getFileName().toString();
                String table = tableDir.getFileName().toString().split("-")[0];
                failedImports.addAll(importData(keyspace, table, tableDir.toString()));
            }
        } else {
            recursiveMove(Paths.get(srcDir), Paths.get(configuration.getDataFileLocation()));
        }
        return failedImports;
    }

    private List<String> importData(String keyspace, String table, String source)
            throws IOException {
        try (JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
            return nodeTool.importNewSSTables(
                    keyspace,
                    table,
                    ImmutableSet.of(source),
                    false /* resetLevel */,
                    false /* clearRepaired */,
                    true /* verifySSTables */,
                    true /* verifyTokens */,
                    true /* invalidateCaches */,
                    false /* extendedVerify */,
                    false /* copyData */);
        }
    }

    private void recursiveMove(Path source, Path destination) throws IOException {
        Preconditions.checkState(Files.exists(source));
        if (!Files.exists(destination)) {
            if (!destination.toFile().mkdirs()) {
                throw new IOException("Failed creating " + destination);
            }
        }
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(source)) {
            for (Path path : directoryStream) {
                if (Files.isRegularFile(path)) {
                    Files.move(path, destination.resolve(path.getFileName()));
                } else if (Files.isDirectory(path)) {
                    recursiveMove(path, destination.resolve(path.getFileName()));
                } else {
                    throw new IOException("Failed determining type of inode is " + path);
                }
            }
        }
    }
}
