package com.netflix.priam.backupv2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.ImplementedBy;

import java.time.Instant;

/**
 * Interface that reads MetaFiles.
 */
@ImplementedBy(MetaFileReaderImpl.class)
public interface IMetaFileReader {
    /**
     *  Downloads, optionally decompresses, deserializes the Json and extracts from it
     *  th S3 object keys of the sstable componentns contained within the meta file
     *  found at metaFileKey.
     *
     * @param metaFileKey the S3 object key for the meta file you would like to read.
     * @return
     * @throws Exception
     */
    ImmutableSet<String> read(String metaFileKey) throws Exception;

    /**
     * Gets a list of Meta files starting with tokenPrefix that were published
     *
     * @param tokenPrefix the string prefixing one specific nodes backups e.g, test_backup/1234_cass_foo/98765
     * @return
     * @throws Exception
     */
    ImmutableList<String> getMetas(String tokenPrefix, Instant end) throws Exception;
}
