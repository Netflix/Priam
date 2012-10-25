package com.netflix.priam.utils;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Guice factory for {@link TokenManager}.
 */
public class TokenManagerProvider implements Provider<TokenManager> {
    private final CassandraConfiguration _cassandraConfiguration;

    @Inject
    public TokenManagerProvider(CassandraConfiguration cassandraConfiguration) {
        _cassandraConfiguration = cassandraConfiguration;
    }

    @Override
    public TokenManager get() {
        IPartitioner partitioner;
        try {
            partitioner = FBUtilities.newPartitioner(_cassandraConfiguration.getPartitionerClassName());
        } catch (ConfigurationException e) {
            throw Throwables.propagate(e);
        }
        if (partitioner instanceof RandomPartitioner) {
            return new RandomPartitionerTokenManager();
        }
        if (partitioner instanceof ByteOrderedPartitioner) {
            return new ByteOrderedPartitionerTokenManager(_cassandraConfiguration);
        }
        throw new UnsupportedOperationException(partitioner.getClass().getName());
    }
}
