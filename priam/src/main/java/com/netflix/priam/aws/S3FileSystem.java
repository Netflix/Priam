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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.io.IOUtils;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.RangeReadInputStream;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;

/**
 * Implementation of IBackupFileSystem for S3
 */
@Singleton
public class S3FileSystem extends S3FileSystemBase implements IBackupFileSystem, S3FileSystemMBean
{
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
    
    private static final long UPLOAD_TIMEOUT = (2 * 60 * 60 * 1000L);
    
    private final Provider<AbstractBackupPath> pathProvider;
    private final ICompression compress;
    private final IConfiguration config;
    private BlockingSubmitThreadPoolExecutor executor;
    private RateLimiter rateLimiter;

    @Inject
    public S3FileSystem(Provider<AbstractBackupPath> pathProvider, ICompression compress, final IConfiguration config, ICredential cred)
    {
        this.pathProvider = pathProvider;
        this.compress = compress;
        this.config = config;
        int threads = config.getMaxBackupUploadThreads();
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(threads);
        this.executor = new BlockingSubmitThreadPoolExecutor(threads, queue, UPLOAD_TIMEOUT);
        double throttleLimit = config.getUploadThrottle();
        rateLimiter = RateLimiter.create(throttleLimit < 1 ? Double.MAX_VALUE : throttleLimit);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = MBEAN_NAME;
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }        
        
        super.s3Client = new AmazonS3Client(cred.getAwsCredentialProvider());
        super.s3Client.setEndpoint(super.getS3Endpoint(this.config));
    }

    @Override
    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException
    {
        try
        {
            logger.info("Downloading " + path.getRemotePath() + " from S3 bucket " + getPrefix(this.config));
            downloadCount.incrementAndGet();
            final AmazonS3 client = super.getS3Client();
            long contentLen = client.getObjectMetadata(getPrefix(this.config), path.getRemotePath()).getContentLength();
            path.setSize(contentLen);
            RangeReadInputStream rris = new RangeReadInputStream(client, getPrefix(this.config), path);
            final long bufSize = MAX_BUFFERED_IN_STREAM_SIZE > contentLen ? contentLen : MAX_BUFFERED_IN_STREAM_SIZE;
            
            compress.decompressAndClose(new BufferedInputStream(rris, (int)bufSize), os);
            
            bytesDownloaded.addAndGet(contentLen);
        }
        catch (Exception e)
        {

            throw new BackupRestoreException(e.getMessage(), e);
        }
    }
    
    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException
    {
        super.uploadCount.incrementAndGet();
        AmazonS3 s3Client = super.getS3Client();
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), path.getRemotePath());
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        DataPart part = new DataPart(config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
        List<PartETag> partETags = Lists.newArrayList();
        long chunkSize = config.getBackupChunkSize();
        if (path.getSize() > 0)
            chunkSize = (path.getSize() / chunkSize >= MAX_CHUNKS) ? (path.getSize() / (MAX_CHUNKS - 1)) : chunkSize;
        logger.info(String.format("Uploading to %s/%s with chunk size %d", config.getBackupPrefix(), path.getRemotePath(), chunkSize));
        try
        {
            Iterator<byte[]> chunks = compress.compress(in, chunkSize);
            // Upload parts.
            int partNum = 0;
            while (chunks.hasNext())
            {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length);
                DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);
                executor.submit(partUploader);
                bytesUploaded.addAndGet(chunk.length);
            }
            executor.sleepTillEmpty();
            if (partNum != partETags.size())
                throw new BackupRestoreException("Number of parts(" + partNum + ")  does not match the uploaded parts(" + partETags.size() + ")");
            new S3PartUploader(s3Client, part, partETags).completeUpload();
            
            if (logger.isDebugEnabled())
            {	
               final S3ResponseMetadata responseMetadata = s3Client.getCachedResponseMetadata(initRequest);
               final String requestId = responseMetadata.getRequestId(); // "x-amz-request-id" header
               final String hostId = responseMetadata.getHostId(); // "x-amz-id-2" header
               logger.debug("S3 AWS x-amz-request-id[" + requestId + "], and x-amz-id-2[" + hostId + "]");
            }  
            
        }
        catch (Exception e)
        {
            new S3PartUploader(s3Client, part, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file " + path.getFileName(), e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    @Override
    public int getActivecount()
    {
        return executor.getActiveCount();
    }

    @Override
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till)
    {
        return new S3FileIterator(pathProvider, super.getS3Client(), path, start, till);
    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date)
    {
        return new S3PrefixIterator(config, pathProvider, super.getS3Client(), date);
    }

    /**
     * Note: Current limitation allows only 100 object expiration rules to be
     * set. Removes the rule is set to 0.
     */
    @Override
    public void cleanup()
    {
    	super.cleanUp(this.config, this.pathProvider);
    }

    /*
     * A means to change the default handle to the S3 client.
     */
    public void setS3Client(AmazonS3Client client) {
    	super.s3Client = client;
    }

    public void shutdown()
    {
        if (executor != null)
            executor.shutdown();
    }

    @Override
    public int downloadCount()
    {
        return downloadCount.get();
    }

    @Override
    public int uploadCount()
    {
        return super.uploadCount.get();
    }

    @Override
    public long bytesUploaded()
    {
        return super.bytesUploaded.get();
    }

    @Override
    public long bytesDownloaded()
    {
        return bytesDownloaded.get();
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

}