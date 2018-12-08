/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.priam.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import mockit.Mock;
import mockit.MockUp;

/** Created by aagrawal on 12/6/18. */
public class MockAmazonS3Client extends MockUp<AmazonS3Client> {
    @Mock
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
            throws AmazonClientException {
        ObjectListing listing = new ObjectListing();
        listing.setBucketName(listObjectsRequest.getBucketName());
        listing.setPrefix(listObjectsRequest.getPrefix());
        return listing;
    }

    @Mock
    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
            throws AmazonClientException {
        ObjectListing listing = new ObjectListing();
        listing.setBucketName(previousObjectListing.getBucketName());
        listing.setPrefix(previousObjectListing.getPrefix());
        return new ObjectListing();
    }
}
