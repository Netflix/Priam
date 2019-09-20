package com.netflix.priam.backup;
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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import javax.crypto.KeyGenerator;

public class s3test {
    private static SSECustomerKey SSE_KEY;
    private static AmazonS3 S3_CLIENT;
    private static KeyGenerator KEY_GENERATOR;

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "nflx-cass-primary-persistence-test-us-east-1";
        String bucketNoVersion = "useast1-cass-test-1";
        String keyName = "sample.txt";
        String uploadFileName = "priam/src/test/resources/gossipInfoSample_1.txt";
        String targetKeyName = "perf_aa_sample2.txt";

        // Create an encryption key.
        KEY_GENERATOR = KeyGenerator.getInstance("AES");
        KEY_GENERATOR.init(256, new SecureRandom());
        SSE_KEY = new SSECustomerKey(KEY_GENERATOR.generateKey());

        try {
            S3_CLIENT =
                    AmazonS3ClientBuilder.standard()
                            .withCredentials(new ProfileCredentialsProvider())
                            .withRegion(clientRegion)
                            .build();

            System.out.println(
                    String.format(
                            "bucket: %s, Versioning: %s",
                            bucketName,
                            S3_CLIENT.getBucketVersioningConfiguration(bucketName).getStatus()));
            System.out.println(
                    String.format(
                            "bucket: %s, Versioning: %s",
                            bucketNoVersion,
                            S3_CLIENT
                                    .getBucketVersioningConfiguration(bucketNoVersion)
                                    .getStatus()));

            // Upload an object.
            // uploadObject(bucketName, keyName, new File(uploadFileName));

            // Download the object.
            // downloadObject(bucketName, keyName);

            // Verify that the object is properly encrypted by attempting to retrieve it
            // using the encryption key.
            // retrieveObjectMetadata(bucketName, keyName);
            // retrieveObjectMetadata(bucketName, "perf_aa_sample.txt");
            deleteObject(bucketName, keyName);

            // We put a delete marker on `sample.txt`. This should get automatically deleted by
            // 12th. There are 3 versions, they should get dleted too.
            // ample.txt has 2 versions (should be 1)
            // perf_aa_sample.txt has 3 versions (should be 1)

            // Copy the object into a new object that also uses SSE-C.
            // copyObject(bucketName, keyName, targetKeyName);
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    private static void uploadObject(String bucketName, String keyName, File file) {
        Instant instant = Instant.now().plus(1, ChronoUnit.DAYS);
        PutObjectRequest putRequest =
                new PutObjectRequest(
                        bucketName,
                        keyName,
                        file); // .withSSECustomerKey(SSE_KEY).withObjectLockMode(ObjectLockMode.COMPLIANCE).withObjectLockRetainUntilDate(new Date(instant.toEpochMilli()));
        S3_CLIENT.putObject(putRequest);
        System.out.println("Object uploaded");
    }

    private static void downloadObject(String bucketName, String keyName) throws IOException {
        GetObjectRequest getObjectRequest =
                new GetObjectRequest(bucketName, keyName); // .withSSECustomerKey(SSE_KEY);
        S3Object object = S3_CLIENT.getObject(getObjectRequest);

        System.out.println("Object content: ");
        displayTextInputStream(object.getObjectContent());
    }

    private static void retrieveObjectMetadata(String bucketName, String keyName) {
        GetObjectMetadataRequest getMetadataRequest =
                new GetObjectMetadataRequest(bucketName, keyName); // .withSSECustomerKey(SSE_KEY);
        ObjectMetadata objectMetadata = S3_CLIENT.getObjectMetadata(getMetadataRequest);
        System.out.println(
                String.format(
                        "Metadata retrieved. %s, %s, %s",
                        objectMetadata.getVersionId(),
                        objectMetadata.getObjectLockMode(),
                        objectMetadata.getObjectLockRetainUntilDate()));
    }

    private static void deleteObject(String bucketName, String keyName) {
        //        ListVersionsRequest request = new ListVersionsRequest()
        //                .withBucketName(bucketName).withPrefix(keyName);
        //
        //        VersionListing versionListing = S3_CLIENT.listVersions(request);
        //        int numVersions = 0, numPages = 0;
        //        while (true) {
        //            numPages++;
        //            for (S3VersionSummary objectSummary :
        //                    versionListing.getVersionSummaries()) {
        //                System.out.printf("Retrieved object %s, version %s\n",
        //                        objectSummary.getKey(),
        //                        objectSummary.getVersionId());
        //                numVersions++;
        //            }
        //            // Check whether there are more pages of versions to retrieve. If
        //            // there are, retrieve them. Otherwise, exit the loop.
        //            if (versionListing.isTruncated()) {
        //                versionListing = S3_CLIENT.listNextBatchOfVersions(versionListing);
        //            } else {
        //                break;
        //            }
        //        }

        //        GetObjectMetadataRequest getMetadataRequest =
        ////                new GetObjectMetadataRequest(bucketName,
        // keyName);//.withSSECustomerKey(SSE_KEY);
        ////        ObjectMetadata objectMetadata = S3_CLIENT.getObjectMetadata(getMetadataRequest);
        ////        DeleteVersionRequest deleteVersionRequest = new DeleteVersionRequest(bucketName,
        // keyName, objectMetadata.getVersionId());
        ////        S3_CLIENT.deleteVersion(deleteVersionRequest);

        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, keyName);
        S3_CLIENT.deleteObject(deleteObjectRequest);
    }

    private static void copyObject(String bucketName, String keyName, String targetKeyName)
            throws NoSuchAlgorithmException {
        // Create a new encryption key for target so that the target is saved using SSE-C.
        SSECustomerKey newSSEKey = new SSECustomerKey(KEY_GENERATOR.generateKey());

        CopyObjectRequest copyRequest =
                new CopyObjectRequest(bucketName, keyName, bucketName, targetKeyName)
                        .withSourceSSECustomerKey(SSE_KEY)
                        .withDestinationSSECustomerKey(newSSEKey);

        S3_CLIENT.copyObject(copyRequest);
        System.out.println("Object copied");
    }

    private static void displayTextInputStream(S3ObjectInputStream input) throws IOException {
        // Read one line at a time from the input stream and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }
}
