package org.apache.cassandra.io.sstable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.cassandra.io.sstable.SSTableLoader.Client;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.FileStreamTask;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.streaming.PendingFile;
import org.apache.cassandra.streaming.StreamHeader;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Singleton
public class SSTableLoaderWrapper {
    private static final Logger logger = LoggerFactory.getLogger(SSTableLoaderWrapper.class);
    private static Set<Component> allComponents = Sets.newHashSet(Component.COMPRESSION_INFO, Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.STATS, Component.DIGEST);

    private final OutputHandler options = new OutputHandler() {
        public void output(String msg) {
            logger.info(msg + "\n");
        }

        public void debug(String msg) {
            logger.debug(msg + "\n");
        }

        public void warn(String msg) {
            logger.warn(msg + "\n");
        }

        public void warn(String msg, Throwable th) {
            logger.warn(msg + "\n", th);
        }
    };

    /**
     * Not multi-threaded intentionally.
     *
     * @return
     */
    public Collection<PendingFile> stream(File directory) throws IOException, InterruptedException {
        Client client = new Client() {
            public boolean validateColumnFamily(String keyspace, String cfName) {
                return true;
            }

            public void init(String keyspace) {
            }
        };
        SSTableLoader loader = new SSTableLoader(directory, client, options);
        Collection<PendingFile> pendingFiles = Lists.newArrayList();
        for (SSTableReader sstable : loader.openSSTables()) {
            Descriptor desc = sstable.descriptor;
            List<Pair<Long, Long>> sections = Lists.newArrayList(new Pair<Long, Long>(0L, sstable.onDiskLength()));
            PendingFile pending = new PendingFile(sstable, desc, SSTable.COMPONENT_DATA, sections, OperationType.BULK_LOAD, sstable.estimatedKeys());
            StreamHeader header = new StreamHeader(directory.getName(), System.nanoTime(), pending, Collections.singleton(pending));
            logger.info("Streaming to {}", InetAddress.getLocalHost());
            new FileStreamTask(header, InetAddress.getLocalHost()).run();
            logger.info("Done Streaming: " + pending.toString());
            sstable.releaseReference();
            pendingFiles.add(pending);
        }
        return pendingFiles;
    }

    public void deleteCompleted(Collection<PendingFile> sstables) throws IOException {
        logger.info("Restored SST's Now Deleting: " + StringUtils.join(sstables, ","));
        for (PendingFile file : sstables) {
            for (Component component : allComponents) {
                FileUtils.delete(file.sstable.descriptor.filenameFor(component));
            }
        }
    }
}
