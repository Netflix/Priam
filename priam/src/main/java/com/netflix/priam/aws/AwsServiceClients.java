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
package com.netflix.priam.aws;

import java.net.InetAddress;
import java.net.UnknownHostException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;


/**
 * Static facade to create AWS service clients with appropriately configured endpoints.
 * 
 * Endpoints constructed according to: {@link http://docs.aws.amazon.com/general/latest/gr/rande.html}.
 */
public class AwsServiceClients {
  
  private static boolean isRegion(String region) {
    try {
      InetAddress.getByName( "ec2." + region + ".amazonaws.com" );
      return true;
    } catch ( UnknownHostException ex ) {
      return false;
    }
  }
  
  
  public static AmazonAutoScaling autoScaling(AWSCredentialsProvider provider, String region) {
    AmazonAutoScaling client = new AmazonAutoScalingClient(provider);
    if (isRegion( region )) {
      client.setEndpoint("autoscaling." + region + ".amazonaws.com");
    } else {
      client.setEndpoint( region + ":8773/services/AutoScaling" );
    }
    return client;
  }
  
  public static AmazonEC2 ec2(AWSCredentialsProvider provider, String region) {
    AmazonEC2 client = new AmazonEC2Client(provider);
    if (isRegion( region )) {
      client.setEndpoint("ec2." + region + ".amazonaws.com");
    } else {
      client.setEndpoint( region + ":8773/services/Eucalyptus" );
    }
    return client;
  }
 
  public static AmazonS3 s3(AWSCredentialsProvider provider, String region) {
    AmazonS3 client = new AmazonS3Client(provider);
    if ("us-east-1".equals(region)) {
      client.setEndpoint("s3.amazonaws.com");
    } else if (isRegion( region )) {
      client.setEndpoint("s3-" + region + ".amazonaws.com");
    } else {
      client.setEndpoint( region + ":8773/services/Walrus" );
    }
    return client;
  }
}
