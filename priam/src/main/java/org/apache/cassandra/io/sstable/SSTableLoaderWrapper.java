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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader.Client;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.ITransportFactory;
import org.apache.cassandra.thrift.TFramedTransportFactory;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
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

    private final OutputHandler outputHandler = new OutputHandler()
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
     * @return
     */
    public Collection<String> stream(File directory, int throttleMb) throws IOException, InterruptedException
    {
    	InetAddress host = InetAddress.getLocalHost();
        Client client = new ExternalClient(
        	ImmutableSet.of(host),
        	config.getThriftPort(),
        	null,
        	null,
        	new TFramedTransportFactory()
        );

        SSTableLoader loader = new SSTableLoader(directory, client, outputHandler);
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(throttleMb);	// TODO: make configurable

        FileCompletedListener listener = new FileCompletedListener(host);
        StreamResultFuture future = loader.stream(Collections.<InetAddress>emptySet(), listener);

        try
        {
            future.get();
        }
        catch (Exception e) {
        	logger.error("Failed streaming to the following hosts: " + loader.getFailedHosts(), e);
        }

        return listener.getCompletedFiles();
    }

    public void deleteCompleted(Collection<String> filenames) throws IOException
    {
        logger.info("Restored SST's Now Deleting: " + StringUtils.join(filenames, ","));
        for (String filename : filenames)
        {
        	Descriptor desc = Descriptor.fromFilename(filename);

            for (Component component : allComponents)
                FileUtils.delete(desc.filenameFor(component));
        }
    }

    static class FileCompletedListener implements StreamEventHandler
    {
    	private Set<String> completedFiles = Sets.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    	private final InetAddress host;

    	public FileCompletedListener(InetAddress host)
    	{
    		this.host = host;
    	}

    	public Collection<String> getCompletedFiles()
    	{
    		return completedFiles;
    	}

		@Override
		public void onSuccess(StreamState result) { }

		@Override
		public void onFailure(Throwable t) { }

		@Override
		public void handleStreamEvent(StreamEvent event)
		{
			if (event.eventType == StreamEvent.Type.FILE_PROGRESS)
			{
				ProgressInfo progress = ((StreamEvent.ProgressEvent) event).progress;

				if (progress.peer != host)
				{
					logger.warn("Ignoring progress about unexpected host " + progress.peer);
					return;
				}

				if (progress.currentBytes == progress.totalBytes)
				{
					logger.info("Completed streaming of file: " + progress.fileName);
					completedFiles.add(progress.fileName);
				}
			}
		}
    }


    // Cloned from Cassandra's BulkLoader.java because it's protected.
    static class ExternalClient extends SSTableLoader.Client
    {
        private final Map<String, CFMetaData> knownCfs = new HashMap<>();
        private final Set<InetAddress> hosts;
        private final int rpcPort;
        private final String user;
        private final String passwd;
        private final ITransportFactory transportFactory;

        public ExternalClient(Set<InetAddress> hosts, int port, String user, String passwd, ITransportFactory transportFactory)
        {
            super();
            this.hosts = hosts;
            this.rpcPort = port;
            this.user = user;
            this.passwd = passwd;
            this.transportFactory = transportFactory;
        }

        public void init(String keyspace)
        {
            Iterator<InetAddress> hostiter = hosts.iterator();
            while (hostiter.hasNext())
            {
                try
                {
                    // Query endpoint to ranges map and schemas from thrift
                    InetAddress host = hostiter.next();
                    Cassandra.Client client = createThriftClient(host.getHostAddress(), rpcPort, this.user, this.passwd, this.transportFactory);

                    setPartitioner(client.describe_partitioner());
                    Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                    for (TokenRange tr : client.describe_ring(keyspace))
                    {
                        Range<Token> range = new Range<>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
                        for (String ep : tr.endpoints)
                        {
                            addRangeForEndpoint(range, InetAddress.getByName(ep));
                        }
                    }

                    String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s'",
                                                 Keyspace.SYSTEM_KS,
                                                 SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                                                 keyspace);
                    CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
                    for (CqlRow row : result.rows)
                    {
                        CFMetaData metadata = CFMetaData.fromThriftCqlRow(row);
                        knownCfs.put(metadata.cfName, metadata);
                    }
                    break;
                }
                catch (Exception e)
                {
                    if (!hostiter.hasNext())
                        throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
                }
            }
        }

        public CFMetaData getCFMetaData(String keyspace, String cfName)
        {
            return knownCfs.get(cfName);
        }

        private static Cassandra.Client createThriftClient(String host, int port, String user, String passwd, ITransportFactory transportFactory) throws Exception
        {
            TTransport trans = transportFactory.openTransport(host, port);
            TProtocol protocol = new TBinaryProtocol(trans);
            Cassandra.Client client = new Cassandra.Client(protocol);
            if (user != null && passwd != null)
            {
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put(IAuthenticator.USERNAME_KEY, user);
                credentials.put(IAuthenticator.PASSWORD_KEY, passwd);
                AuthenticationRequest authenticationRequest = new AuthenticationRequest(credentials);
                client.login(authenticationRequest);
            }
            return client;
        }
    }

}
