package com.netflix.priam.google;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.cryptography.IKeyCryptography;
import com.netflix.priam.restore.GoogleRestoreStrategy;

public class GoogleEncryptedFileSystem implements IBackupFileSystem, GoogleFileSystemMBean {

	private static final Logger logger = LoggerFactory.getLogger(GoogleEncryptedFileSystem.class);
	
	private static final String APPLICATION_NAME = "gdl";
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	
	private HttpTransport httpTransport;
	private Credential credential; //represents our "service account" credentials we will use to access GCS
	private Storage gcsStorageHandle;
	private Storage.Objects objectsResoruceHandle = null;

	private Provider<AbstractBackupPath> pathProvider;
	private String srcBucketName;
	private IConfiguration config;
	private AtomicInteger downloadCount = new AtomicInteger();

	private IKeyCryptography keyCryptography;
	
	@Inject
	public GoogleEncryptedFileSystem(Provider<AbstractBackupPath> pathProvider, final IConfiguration config
			, @Named("keycryptography") IKeyCryptography keyCryptography) {
		
		this.pathProvider = pathProvider;
		this.config = config;
		this.keyCryptography = keyCryptography;
		
		try {
	        
			this.httpTransport = GoogleNetHttpTransport.newTrustedTransport();
	        
		} catch (Exception e) {
			throw new IllegalStateException("Unable to create a handle to the Google Http tranport", e);
		}
		
		this.srcBucketName = GoogleRestoreStrategy.getSourcebucket(getPathPrefix());
		
	}
	
	private Storage.Objects constructObjectResourceHandle() {
		if (this.objectsResoruceHandle != null) {
			return this.objectsResoruceHandle;
		}
		
		constructGcsStorageHandle();
		
		this.objectsResoruceHandle = this.gcsStorageHandle.objects();
		
		return this.objectsResoruceHandle;
	}

    /*
     * Get a handle to the GCS api to manage our data within their storage.  Code derive from
     * https://code.google.com/p/google-api-java-client/source/browse/storage-cmdline-sample/src/main/java/com/google/api/services/samples/storage/cmdline/StorageSample.java?repo=samples
     * 
     * Note: GCS storage will use our credential to do auto-refresh of expired tokens
     */

	private Storage constructGcsStorageHandle() {
		if (this.gcsStorageHandle != null) {
			return this.gcsStorageHandle;
		}
		
		try {
			
			constructGcsCredential();							
			
		} catch (Exception e) {
			throw new IllegalStateException("Exception during GCS authorization", e);			
		}	
		
		this.gcsStorageHandle = new Storage.Builder(this.httpTransport, JSON_FACTORY, this.credential).setApplicationName(APPLICATION_NAME).build();
		return this.gcsStorageHandle;
	}
	
	/** Authorizes the installed application to access user's protected data, code from https://developers.google.com/maps-engine/documentation/oauth/serviceaccount
	 * and http://javadoc.google-api-java-client.googlecode.com/hg/1.8.0-beta/com/google/api/client/googleapis/auth/oauth2/GoogleCredential.html 
	 */
	private Credential constructGcsCredential() throws Exception {
		
		if (this.credential != null) {
			return this.credential;
		}
		
		synchronized(this) {
			
			if (this.credential == null) {
				
				String service_acct_email = this.keyCryptography.decrypt(config.getGcsServiceAccountId());
				
				File gcsPrivateKeyHandle = new File("/apps/tomcat/conf/key.p12");
				OutputStream os = new FileOutputStream(gcsPrivateKeyHandle);
				BufferedOutputStream bos = new BufferedOutputStream(os);
				ByteArrayOutputStream byteos = new ByteArrayOutputStream();
				
				byte[] gcsPrivateKeyPlainText = this.keyCryptography.decrypt(new File(this.config.getGcsServiceAccountPrivateKeyLoc()));
				try {
					
					byteos.write(gcsPrivateKeyPlainText);
					byteos.writeTo(bos);
					
				} catch (IOException e) {
					
					throw new IOException("Exception when writing decrypted gcs private key value to disk.", e);
					
				} finally {
					try {
						bos.close();
					} catch (IOException e) {
						throw new IOException("Exception when closing decrypted gcs private key value to disk.", e);
					}
				}	
				
				Collection<String> scopes = new ArrayList<String>(1);
				scopes.add(StorageScopes.DEVSTORAGE_READ_ONLY);
				this.credential = new GoogleCredential.Builder().setTransport(this.httpTransport)
					      .setJsonFactory(JSON_FACTORY)
					      .setServiceAccountId(service_acct_email)
					      .setServiceAccountScopes(scopes)
					      .setServiceAccountPrivateKeyFromP12File(gcsPrivateKeyHandle)  //Cryptex decrypted service account key derive from the GCS console
					      .build();
			}			
			
		}
		
		return this.credential;
	}	
	
	@Override
	public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException {

		logger.info("Downloading " + path.getRemotePath() + " from GCS bucket " + this.srcBucketName);
		this.downloadCount.incrementAndGet();
		
		String objectName = parseObjectname(getPathPrefix());
		
		com.google.api.services.storage.Storage.Objects.Get get = null;
		try {

			get = constructObjectResourceHandle().get(this.srcBucketName, path.getRemotePath());
			
		} catch (IOException e) {
			throw new BackupRestoreException("IO error retrieving metadata for: " + objectName + " from bucket: " + this.srcBucketName, e);
		}		
		
		get.getMediaHttpDownloader().setDirectDownloadEnabled(true);  // If you're not using GCS' AppEngine, download the whole thing (instead of chunks) in one request, if possible.
		InputStream is = null;
		try {
			 
			is = get.executeMediaAsInputStream();
			IOUtils.copyLarge(is, os);

			
		} catch (IOException e) {
			throw new BackupRestoreException("IO error during streaming of object: " + objectName + " from bucket: " + this.srcBucketName, e);
		} catch (Exception ex) {
        	
        	throw new BackupRestoreException("Exception encountered when copying bytes from input to output", ex);
        	
        } finally
        {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }		
		
	}

	@Override
	public void download(AbstractBackupPath path, OutputStream os, String filePath) throws BackupRestoreException {
		try {
			
			download(path, os);
			
		} catch (Exception e) {
			throw new BackupRestoreException(e.getMessage(), e);
		}
	}

	@Override
	public void upload(AbstractBackupPath path, InputStream in)
			throws BackupRestoreException {
		throw new UnsupportedOperationException ();

	}

	@Override
	public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
		return new GoogleFileIterator(pathProvider, constructGcsStorageHandle(), path, start, till);
	}

	@Override
	public Iterator<AbstractBackupPath> listPrefixes(Date date) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public int getActivecount() {
		// TODO Auto-generated method stub
		return 0;
		
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

	@Override
	public int downloadCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int uploadCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long bytesUploaded() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long bytesDownloaded() {
		// TODO Auto-generated method stub
		return 0;
	}
	
    /**
     * Get restore prefix which will be used to locate GVS files
     */
    public String getPathPrefix() {
    	
        String prefix;
        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();

        return prefix;
    }  

	/*
	 * @param pathPrefix
	 * @return objectName
	 */
	public static String parseObjectname(String pathPrefix) {
		int offset = pathPrefix.lastIndexOf(0x2f);
		return pathPrefix.substring(offset + 1);
		
	}    
	

}