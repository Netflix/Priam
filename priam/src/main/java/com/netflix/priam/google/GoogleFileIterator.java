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
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/*
 * Represents a list of objects within Google Cloud Storage (GCS)
 */
public class GoogleFileIterator implements Iterator<String> {
    private Iterator<String> iterator;
    private String bucketName;
    private String prefix;
    private Storage.Objects objectsResoruceHandle = null;
    private Storage.Objects.List listObjectsSrvcHandle = null;
    private com.google.api.services.storage.model.Objects objectsContainerHandle = null;

    public GoogleFileIterator(Storage gcsStorageHandle, String bucket, String prefix) {

        this.objectsResoruceHandle = gcsStorageHandle.objects();
        this.bucketName = bucket;
        this.prefix = prefix;

        try { // == Get the initial page of results
            this.iterator = createIterator();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception encountered fetching elements, msg: ." + e.getLocalizedMessage(), e);
        }
    }

    private void initListing() {
        try {

            this.listObjectsSrvcHandle =
                    objectsResoruceHandle.list(bucketName); // == list objects within bucket
            // fetch elements within bucket that matches this prefix
            this.listObjectsSrvcHandle.setPrefix(this.prefix);
        } catch (IOException e) {
            throw new RuntimeException("Unable to get gcslist handle to bucket: " + bucketName, e);
        }
    }

    /*
     * Fetch a page of results
     */
    private Iterator<String> createIterator() throws Exception {
        if (listObjectsSrvcHandle == null) initListing();
        List<String> temp = Lists.newArrayList(); // a container of results

        // Sends the metadata request to the server and returns the parsed metadata response.
        this.objectsContainerHandle = listObjectsSrvcHandle.execute();

        for (StorageObject object : this.objectsContainerHandle.getItems()) {
            // processing a page of results
            temp.add(object.getName());
        }
        return temp.iterator();
    }

    @Override
    public boolean hasNext() {
        if (this.iterator.hasNext()) {
            return true;
        }

        while (this.objectsContainerHandle.getNextPageToken() != null && !iterator.hasNext())
            try { // if here, you have iterated through all elements of the previous page, now, get
                // the next page of results
                this.listObjectsSrvcHandle.setPageToken(objectsContainerHandle.getNextPageToken());
                this.iterator = createIterator();
            } catch (Exception e) {
                throw new RuntimeException(
                        "Exception encountered fetching elements, see previous messages for details.",
                        e);
            }

        return this.iterator.hasNext();
    }

    @Override
    public String next() {
        return iterator.next();
    }
}
