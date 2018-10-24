# Installation
Priam provides several default implementations (AWS, Configuration, credentials etc). You can use these or choose to create your own.

* Build the code
* Set up your auto-scale group (ASG) and spin up instances
* Install Cassandra and web container (such as tomcat) on your instances.
* Setup aws credentials and SimpleDB properties
* Create S3 buckets to store your backups
* Copy priam-cass-extensions-<version>.jar into your $CASS_HOME/lib directory
* Add -javaagent:`$CASS_HOME/lib/priam-cass-extensions-<version>.jar` to cassandra's JVM arguments
* Configure basic configurations
* Deploy Priam.war in your container

## Build Process
Checkout the code from git and run:
`/gradlew build`
The gradlew script will pull down all necessary gradle components/infrastructure automatically, then run the build. 

This should create both a jar and a war file for your project. Note that, the default provided log4j.properties assumes tomcat deployment. Modify this according to your needs.

Priam uses Google Guice. You can override several of the default implementations and bind your implementations in the provided Guice module (see PriamGuiceModule.java)

## [Auto Scaling Group](http://aws.amazon.com/autoscaling/) setup
When setting up ASG, using as-create-auto-scaling-group, set availability zone to single zone (--availability-zones). For high availability, set multiple ASGs with one zone per ASG.

### Choosing ASG name

Your Cassandra cluster could be spanning multiple ASGs. Such cases arise when you want to provide HA across zones and to overcome the AWS limitation of load balancing across zones i.e., currently, AWS does not guarantee instances will be balanced across zones if instances are not available in a particular zone. In such cases, you could create multiple ASGs with each ASG bound to single zone.

In such cases your ASG name should be suffixed with `'-{ZONE}'`. Eg: `test_cluster-useast1a.`

## Web Container setup

Since Priam changes the configuration files for Cassandra and starts/stops the services, the web container it's running in must have execute rights on the script to modify the cassandra.yaml file and execute the `/etc/init.d/cassandra` (configurable location) script.

## Credentials


The default implementation uses clear text credentials. To use this provide AWS accessid and secrectkey in `-/etc/awscredential.properties` -- copy and modify from `src/main/resources/conf/awscredential.properties`. 

You can however, override ICredential to provide a more secure way of obtaining credentials. Additionally, ensure your keys do not contain special characters and are not enclosed in quotes of any kind. 

The preferred way is to use IAM key profile management. 

### IAMCredential
Using IAM Credentials allows you to provide access to the AWS api without storing an AccessKeyId or SecretAccessKey anywhere on the machine.

1. Create a new IAM Role in the AWS console or using the API and make sure it can access EC2, S3, and SimpleDB resources.
2. Assign that role to the auto scaling group.
3. Modify `priam/src/main/java/com/netflix/priam/defaultimpl/PriamGuiceModule.java`

```java
   // Add this line
   import com.netflix.priam.aws.IAMCredential;
   
   public class PriamGuiceModule extends AbstractModule {
           bind(IBackupFileSystem.class).annotatedWith(Names.named("backup")).to(S3FileSystem.class);
           bind(IBackupFileSystem.class).annotatedWith(Names.named("incr_restore")).to(S3FileSystem.class);
           bind(IBackupFileSystem.class).annotatedWith(Names.named("backup_status")).to(S3FileSystem.class);
           .....
           // Add this line
           bind(ICredential.class).to(IAMCredential.class);
       }
 ```

   
## S3 Buckets
* Create an S3 bucket to store your backup files. The default bucket name used by PriamConfiguration is cassandra-archive.
* Ensure the Credentials used above has permissions to read/write to this S3 bucket. For best performance (and cost), consider creating one S3 bucket per AWS region. 

## SimpleDB Domains
Priam uses SimpleDB to register nodes as well as read properties.

Create the following SimpleDB domains: InstanceIdentity & PriamProperties. 

Use PriamProperties to add properties if you want to modify defaults. InstanceIdentity is used by Priam to register nodes. The SimpleDB domains must be located in the US-East-1 region.

**Note**: SimpleDB Configuration Management and Token Management is soon to be deprecated and moved to use DynamoDB.

## PriamStartupAgent and NFSeedProvider

These are the 2 classes that will be required by Cassandra for starting and fetching token, seeds and other information from Priam. You will need to copy priam-cass-extensions-<version>.jar into your $CASS_HOME/lib directory, and add -javaagent:$CASS_HOME/lib/priam-cass-extensions-<version>.jar to cassandra's JVM arguments.

Priam updates cassandra.yaml with NFSeedProvider by default.

## Configuration
To change the default Priam properties, you can create property items in the PriamProperties SimpleDB domain. Each property item is defined by the attributes appId, property and value. appId is set to the ASG name by default. When using multiple ASGs, the zone suffix is removed. Refer above for ASG naming convention. Refer to Properties for property name and default value for all property items.

### Providing start and stop scripts
1. Configure the name of the application. 
2. As part of the configuration provide any custom start and stop scripts to Priam via properties. PriamConfiguration defaults to `/etc/init.d/cassandra` script for starting and stopping.    

## Deploying
Copy the jar generated in the build step into the cassandra lib directory. Deploy the war into your web container.

### Verification

To see if Priam has started successfully, you can lookup SimpleDB InstanceIdentity domain for entries for the ASG. Each entry will have instanceid, hostname, token and other details used by the particular node.

You can also verify REST API by running:

```$curl http://localhost:8080/Priam/REST/v1/cassconfig/get_token```

This should return the token used by node. Kudos if you reached so far! Ensure your Cassandra process is up and is using the same token found by running the above command.
