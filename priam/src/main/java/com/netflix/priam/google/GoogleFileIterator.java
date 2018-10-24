/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.google;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Represents a list of objects within Google Cloud Storage (GCS)
 */
public class GoogleFileIterator implements Iterator<AbstractBackupPath> {
    private static final Logger logger = LoggerFactory.getLogger(GoogleFileIterator.class);

    private final Date start;
    private final Date till;
    private Iterator<AbstractBackupPath> iterator;
    private final Provider<AbstractBackupPath> pathProvider;
    private String bucketName;

    private Storage.Objects objectsResoruceHandle = null;
    private Storage.Objects.List listObjectsSrvcHandle = null;
    private com.google.api.services.storage.model.Objects objectsContainerHandle = null;

    private String pathWithinBucket;

    private String nextPageToken;

    /*
     * @param pathProvider
     * @param gcsStorageHandle - a means to perform operations within the destination storage.
     * @param path - metadata about where the object exist within the destination.
     * @param start - timeframe of object to restore
     * @param till - timeframe of object to restore
     */
    public GoogleFileIterator(
            Provider<AbstractBackupPath> pathProvider,
            Storage gcsStorageHandle,
            String path,
            Date start,
            Date till) {

        this.start = start;
        this.till = till;
        this.pathProvider = pathProvider;

        this.objectsResoruceHandle = gcsStorageHandle.objects();

        if (path == null) {
            throw new NullPointerException("Path of object to fetch is null");
        }

        String[] paths = path.split(String.valueOf(S3BackupPath.PATH_SEP));
        if (paths.length < 1) {
            throw new IllegalStateException("Path of object to fetch is invalid.  Path: " + path);
        }

        this.bucketName = paths[0];
        this.pathWithinBucket = pathProvider.get().remotePrefix(start, till, path);

        logger.info(
                "Listing objects from GCS: {}, prefix: {}", this.bucketName, this.pathWithinBucket);

        try {

            this.listObjectsSrvcHandle =
                    objectsResoruceHandle.list(bucketName); // == list objects within bucket

        } catch (IOException e) {
            throw new RuntimeException("Unable to get gcslist handle to bucket: " + bucketName, e);
        }

        // fetch elements within bucket that matches this prefix
        this.listObjectsSrvcHandle.setPrefix(this.pathWithinBucket);

        try { // == Get the initial page of results

            this.iterator = createIterator();

        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception encountered fetching elements, msg: ." + e.getLocalizedMessage(), e);
        }
    }

    /*
     * Fetch a page of results
     */
    private Iterator<AbstractBackupPath> createIterator() throws Exception {
        List<AbstractBackupPath> temp = Lists.newArrayList(); // a container of results

        // Sends the metadata request to the server and returns the parsed metadata response.
        this.objectsContainerHandle = listObjectsSrvcHandle.execute();

        for (StorageObject object :
                this.objectsContainerHandle.getItems()) { // processing a page of results
            String fileName = GoogleEncryptedFileSystem.parseObjectname(object.getName());
            logger.debug(
                    "id: {}, parse file name: {}, name: {}",
                    object.getId(),
                    fileName,
                    object.getName());

            // e.g. of objectname:
            // prod_backup/us-east-1/cass_account/113427455640312821154458202479064646083/201408250801/META/meta.json

            AbstractBackupPath path = pathProvider.get();
            path.parseRemote(object.getName());
            logger.debug(
                    "New key {} path = {} start: {} end: {} my {}",
                    object.getName(),
                    path.getRemotePath(),
                    start,
                    till,
                    path.getTime());
            if ((path.getTime().after(start) && path.getTime().before(till))
                    || path.getTime().equals(start)) {
                temp.add(path);
                logger.debug("Added key {}", object.getName());
            }
        }

        this.nextPageToken = this.objectsContainerHandle.getNextPageToken();

        return temp.iterator();
    }

    @Override
    public boolean hasNext() {
        if (this.iterator == null) return false;

        if (this.iterator.hasNext()) {
            return true;
        }

        if (this.nextPageToken == null) { // there is no additional results
            return false;
        }

        try { // if here, you have iterated through all elements of the previous page, now, get the
            // next page of results

            this.listObjectsSrvcHandle.setPageToken(this.nextPageToken);
            // get the next page of results
            this.iterator = createIterator();

        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception encountered fetching elements, see previous messages for details.",
                    e);
        }

        return this.iterator.hasNext();
    }

    @Override
    public AbstractBackupPath next() {
        return iterator.next();
    }

    @Override
    public void remove() {
        // TODO Auto-generated method stub

    }
}
