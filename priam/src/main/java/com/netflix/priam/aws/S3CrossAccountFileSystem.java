package com.netflix.priam.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.backup.IBackupFileSystem;
import com.amazonaws.services.s3.AmazonS3Client;

/*
 * A version of S3FileSystem which allows it api access across different AWS accounts.
 * 
 * *Note: ideally, this object should extend S3FileSystem but could not be done because:
 * - S3FileSystem is a singleton and it uses DI.  To follow the DI pattern, the best way to get this singleton is via injection.
 * - S3FileSystem registers a MBean to JMX which must be only once per JVM.  If not, you get 
 * java.lang.RuntimeException: javax.management.InstanceAlreadyExistsException: com.priam.aws.S3FileSystemMBean:name=S3FileSystemMBean
 * - 
 */
@Singleton
public class S3CrossAccountFileSystem  {
	private static final Logger logger = LoggerFactory.getLogger(S3CrossAccountFileSystem.class);
	
	private AmazonS3Client s3Client;
	private S3FileSystem s3fs;
	private IConfiguration config;
	private IS3Credential s3Credential;
	
	@Inject
	public S3CrossAccountFileSystem(@Named("backup") IBackupFileSystem fs, @Named("awss3roleassumption") IS3Credential s3Credential, IConfiguration config) {
	
		
		this.s3fs = (S3FileSystem) fs;
		this.config = config;
		this.s3Credential = s3Credential;
		        
	}
	
	public IBackupFileSystem getBackupFileSystem() {
		return this.s3fs;
	}
	
	public AmazonS3Client getCrossAcctS3Client() {
		if (this.s3Client == null ) {
			
			synchronized(this) {
				
				if (this.s3Client == null ) {
				
					try {

						this.s3Client = new AmazonS3Client(s3Credential.getAwsCredentialProvider());

					} catch (Exception e) {
						throw new IllegalStateException("Exception in getting handle to s3 client.  Msg: " + e.getLocalizedMessage(), e);
						
					}
					this.s3Client.setEndpoint(s3fs.getS3Endpoint(config));
					
					//Lets leverage the IBackupFileSystem behaviors except we want it to use our amazon S3 client which has cross AWS account api capability.
					this.s3fs.setS3Client(s3Client);
					
				}		
				
			}
			
		}
		
		
		return this.s3Client;
	}
		
}