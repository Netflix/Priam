package com.netflix.priam.aws;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.netflix.priam.merics.IMetricPublisher;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.RangeReadInputStream;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;

/**
 * Implementation of IBackupFileSystem for S3.  The upload/download will work with ciphertext.
 */
@Singleton
public class S3EncryptedFileSystem extends S3FileSystemBase implements IBackupFileSystem, S3EncryptedFileSystemMBean {

	private static final Logger logger = LoggerFactory.getLogger(S3EncryptedFileSystem.class);
	private static final long UPLOAD_TIMEOUT = (2 * 60 * 60 * 1000L);
	
	private Provider<AbstractBackupPath> pathProvider;
	private ICompression compress;

	private IConfiguration config;
	private BlockingSubmitThreadPoolExecutor executor;
	private RateLimiter rateLimiter; //a throttling mechanism, we can limit the amount of bytes uploaded to endpoint per second.
	private AtomicInteger uploadCount = new AtomicInteger();
	private IFileCryptography encryptor;
	
	@Inject
	public S3EncryptedFileSystem(Provider<AbstractBackupPath> pathProvider, ICompression compress, final IConfiguration config, ICredential cred
			, @Named("filecryptoalgorithm") IFileCryptography fileCryptography
			, @Named("defaultmetricpublisher") IMetricPublisher metricPublisher
			) {

		super(metricPublisher);
        this.pathProvider = pathProvider;
        this.compress = compress;
        this.config = config;
        this.encryptor = fileCryptography;
        
        int threads = config.getMaxBackupUploadThreads();
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(threads);
        this.executor = new BlockingSubmitThreadPoolExecutor(threads, queue, UPLOAD_TIMEOUT);
        
        double throttleLimit = config.getUploadThrottle();
        this.rateLimiter = RateLimiter.create(throttleLimit < 1 ? Double.MAX_VALUE : throttleLimit);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = ENCRYPTED_FILE_SYSTEM_MBEAN_NAME;
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
        	throw new RuntimeException("Unable to regiser JMX bean: " + mbeanName + " to JMX server.  Msg: " + e.getLocalizedMessage(), e);
        }        
        
        super.s3Client = new AmazonS3Client(cred.getAwsCredentialProvider());
        super.s3Client.setEndpoint(super.getS3Endpoint(this.config));        
	}
	
	@Override
	public int downloadCount() {
		return downloadCount.get();
	}

	@Override
	public int uploadCount() {
		return super.uploadCount.get();
	}

	@Override
	/*
    Note:  provides same information as getBytesUploaded() but it's meant for S3FileSystemMBean object types.
     */
	public long bytesUploaded() {
		return super.bytesUploaded.get();
	}

	@Override
	public long getBytesUploaded() {
		return super.bytesUploaded.get();
	}


	@Override
	public long bytesDownloaded() {
		return bytesDownloaded.get();
	}

	@Override
	public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException {
        try
        {
            logger.info("Downloading " + path.getRemotePath() + " from S3 bucket " + getPrefix(this.config));
            downloadCount.incrementAndGet();
            final AmazonS3 client = super.getS3Client();
            long contentLen = client.getObjectMetadata(getPrefix(this.config), path.getRemotePath()).getContentLength();
            path.setSize(contentLen);
            RangeReadInputStream rris = new RangeReadInputStream(client, getPrefix(this.config), path);
            final long bufSize = MAX_BUFFERED_IN_STREAM_SIZE > contentLen ? contentLen : MAX_BUFFERED_IN_STREAM_SIZE;
            
        	/*
        	 * To handle use cases where decompression should be done outside of the download.  For example, the file have been compressed and then encrypted.
        	 * Hence, decompressing it here would compromise the decryption.
        	 */
            try
            {
                IOUtils.copyLarge(rris, os);
                
            } catch (Exception ex) {
            	
            	throw new BackupRestoreException("Exception encountered when copying bytes from input to output", ex);
            	
            } finally
            {
                IOUtils.closeQuietly(rris);
                IOUtils.closeQuietly(os);
            }            
            
            bytesDownloaded.addAndGet(contentLen);
        }
        catch (Exception e)
        {
            throw new BackupRestoreException("Exception encountered downloading " + path.getRemotePath() + " from S3 bucket " + getPrefix(this.config)
            		+ ", Msg: " + e.getMessage(), e);
        }
	}
	
    /**
     * This method does exactly as other download method.(Supposed to be overridden)
     * filePath parameter provides the diskPath of the downloaded file.
     * This path can be used to correlate the files which are Streamed In 
     * during Incremental Restores
     */
	@Override
	public void download(AbstractBackupPath path, OutputStream os,
			String filePath) throws BackupRestoreException {
		try {
			// Calling original Download method
			download(path, os);
		} catch (Exception e) {
			throw new BackupRestoreException(e.getMessage(), e);
		}

	}

	@Override
	public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException {
		reinitialize();  //perform before file upload
		super.uploadCount.incrementAndGet();
		
		//== Setup for multi part (chunks) upload to aws
		AmazonS3 s3Client = super.getS3Client();
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), path.getRemotePath()); //initialize chunking request to aws
		InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest); //Fetch the aws generated upload id for this chunking request
		DataPart part = new DataPart(config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
        List<PartETag> partETags = Lists.newArrayList(); //Metadata on number of parts to be uploaded
        long chunkSize = config.getBackupChunkSize();
        if (path.getSize() > 0)
            chunkSize = (path.getSize() / chunkSize >= MAX_CHUNKS) ? (path.getSize() / (MAX_CHUNKS - 1)) : chunkSize; //compute the size of each block we will upload to endpoint
                      
            
		//== Read chunks from src, compress it, and write to temp file
        String compressedFileName = path.newRestoreFile() + ".compressed";
        logger.debug(String.format("Compressing %s with chunk size %d", compressedFileName, chunkSize));
		File compressedDstFile = null;
		FileOutputStream compressedDstFileOs = null;
		BufferedOutputStream compressedBos = null;
		try {
			
			compressedDstFile = new File(compressedFileName);
			compressedDstFileOs = new FileOutputStream(compressedDstFile);
			compressedBos = new BufferedOutputStream(compressedDstFileOs);
			
		} catch (FileNotFoundException e) {
			throw new BackupRestoreException("Not able to find temporary compressed file: " + compressedFileName);
		} 
		
		try {
	        
			Iterator<byte[]> compressedChunks = this.compress.compress(in, chunkSize);
			while (compressedChunks.hasNext()) {
				byte[] compressedChunk = compressedChunks.next();
				compressedBos.write(compressedChunk);
			}						
			
		} catch (IOException e) {
			System.out.println("Exception in compressing the input data.  Msg: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(compressedBos);			
		}
            
        //== Read compressed data, encrypt each chunk, upload it to aws    
        logger.debug(String.format("Uploading to %s/%s with chunk size %d", config.getBackupPrefix(), path.getRemotePath(), chunkSize));
        
		FileInputStream compressedFileIs = null;
		BufferedInputStream compressedBis = null;        
        try {

			compressedFileIs = new FileInputStream(new File(compressedFileName));
			compressedBis = new BufferedInputStream(compressedFileIs);
            Iterator<byte[]> chunks = this.encryptor.encryptStream(compressedBis, path.getRemotePath());

            int partNum = 0; //identifies this part position in the object we are uploading
            while (chunks.hasNext())
            {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length); //throttle upload to endpoint
                
                DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);
                executor.submit(partUploader);
                
                super.bytesUploaded.addAndGet(chunk.length);
            }        
            
            executor.sleepTillEmpty();
            if (partNum != partETags.size())
                throw new BackupRestoreException("Number of parts(" + partNum + ")  does not match the expected number of uploaded parts(" + partETags.size() + ")");
            
            new S3PartUploader(s3Client, part, partETags).completeUpload(); //complete the aws chunking upload by providing to aws the ETag that uniquely identifies the combined object data       	
        	
        } catch(Exception e ) {
        	new S3PartUploader(s3Client, part, partETags).abortUpload();
        	throw new BackupRestoreException("Error uploading file " + path.getFileName(), e);
        } finally {
			IOUtils.closeQuietly(compressedBis);
			if (compressedDstFile.exists())
				compressedDstFile.delete();
        }
        
	}

	@Override
	public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
		return new S3FileIterator(pathProvider, super.getS3Client(), path, start, till);
	}

	@Override
	public Iterator<AbstractBackupPath> listPrefixes(Date date) {
		return new S3PrefixIterator(config, pathProvider, super.getS3Client(), date);
	}

	@Override
	public void cleanup() {
    	super.cleanUp(this.config, this.pathProvider);

	}

	@Override
	public int getActivecount() {
		return executor.getActiveCount();
	}

	@Override
	public void shutdown() {
        if (executor != null)
            executor.shutdown();

	}

	/*
     * A means to change the default handle to the S3 client.
     */
    public void setS3Client(AmazonS3Client client) {
    	super.s3Client = client;
    }	

}