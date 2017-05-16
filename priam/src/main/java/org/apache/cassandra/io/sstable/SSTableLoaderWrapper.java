/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.io.sstable.SSTableLoader.Client;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.FileStreamTask;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.streaming.PendingFile;
import org.apache.cassandra.streaming.StreamHeader;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.CassandraTuner;

@Singleton
public class SSTableLoaderWrapper
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableLoaderWrapper.class);
    private static Set<Component> allComponents = Sets.newHashSet(Component.COMPRESSION_INFO, Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.STATS, Component.DIGEST);
    private final IConfiguration config;
    
    @Inject
    public SSTableLoaderWrapper(IConfiguration config, CassandraTuner tuner) throws IOException
    {        
        this.config = config;
        String srcCassYamlFile =  config.getCassHome() + "/conf/cassandra.yaml";
        String targetYamlLocation = "/tmp/";
        
        File sourceFile = new File(srcCassYamlFile);
        File targetFile = new File(targetYamlLocation+"incr-restore-cassandra.yaml");
        logger.info("Copying file : " + sourceFile.getName() +" to --> "+targetFile.getName());
      
        //copy file from one location to another
        Files.copy(sourceFile, targetFile);
        
        logger.info("Trying to load the yaml file from: " + targetFile);
        tuner.writeAllProperties(targetFile.getPath(), "localhost", "org.apache.cassandra.locator.SimpleSeedProvider");
        System.setProperty("cassandra.config", "file:"+ targetFile.getPath());
    }

    private final OutputHandler options = new OutputHandler()
    {
        public void output(String msg)
        {
            logger.info(msg + "\n");
        }

        public void debug(String msg)
        {
            logger.debug(msg + "\n");
        }

        public void warn(String msg)
        {
            logger.warn(msg + "\n");
        }

        public void warn(String msg, Throwable th)
        {
            logger.warn(msg, th);
        }
    };

    /**
     * Not multi-threaded intentionally.
     * @return 
     */
    public Collection<PendingFile> stream(File directory) throws IOException, InterruptedException
    {
        Client client = new Client()
        {
            public boolean validateColumnFamily(String keyspace, String cfName)
            {
                return true;
            }

            public void init(String keyspace)
            {
            }
        };
        try {
	    client.setPartitioner(config.getPartitioner());
	} catch (Exception e) {
	    logger.error("Configuration Exception while setting partitioner", e);
	}
        
        
        SSTableLoader loader = new SSTableLoader(directory, client, options);       
        
        Collection<PendingFile> pendingFiles = Lists.newArrayList();
        for (SSTableReader sstable : loader.openSSTables())
        {
            Descriptor desc = sstable.descriptor;
            List<Pair<Long, Long>> sections = Lists.newArrayList(Pair.create(0L, sstable.onDiskLength()));
            PendingFile pending = new PendingFile(sstable,
                    desc,
                    SSTable.COMPONENT_DATA,
                    sections,
                    OperationType.BULK_LOAD,
                    sstable.estimatedKeys(),
                    sstable.compression ?
                            new CompressionInfo(
                                    sstable.getCompressionMetadata().getChunksForSections(sections),
                                    sstable.getCompressionMetadata().parameters) :
                            null);
            StreamHeader header = new StreamHeader(directory.getName(), UUID.randomUUID(), pending, Collections.singleton(pending));
            logger.info("Streaming to {}", InetAddress.getLocalHost());
            new FileStreamTask(header, InetAddress.getLocalHost()).run();
            logger.info("Done Streaming: " + pending.toString());
            sstable.releaseReference();
            pendingFiles.add(pending);
        }
        return pendingFiles;
    }

    public void deleteCompleted(Collection<PendingFile> sstables) throws IOException
    {
        logger.info("Restored SST's Now Deleting: " + StringUtils.join(sstables, ","));
        for (PendingFile file : sstables)
            for (Component component : allComponents)
                FileUtils.delete(file.sstable.descriptor.filenameFor(component));
    }
}
