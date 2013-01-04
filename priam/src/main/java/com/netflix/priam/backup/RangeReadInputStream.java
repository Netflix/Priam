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
package com.netflix.priam.backup;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.netflix.priam.utils.RetryableCallable;

/**
 * An implementation of InputStream that will request explicit byte ranges of the target file.
 * This will make it easier to retry a failed read - which is important if we don't want to \
 * throw away a 100Gb file and restart after reading 99Gb and failing.
 */
public class RangeReadInputStream extends InputStream
{
    private static final Logger logger = LoggerFactory.getLogger(RangeReadInputStream.class);

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final AbstractBackupPath path;
    private long offset;

    public RangeReadInputStream(AmazonS3 s3Client, String bucketName, AbstractBackupPath path)
    {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.path = path;
    }

    public int read(final byte b[], final int off, final int len) throws IOException
    {
//        logger.info(String.format("incoming buf req's size = %d, off = %d, len to read = %d, on file size %d, cur offset = %d path = %s",
//                b.length, off, len, path.getSize(), offset, path.getRemotePath()));
        final long fileSize = path.getSize();
        if(fileSize > 0 && offset >= fileSize)
            return -1;
        final long firstByte = offset;
        long curEndByte = firstByte + len;
        curEndByte = curEndByte <= fileSize ? curEndByte : fileSize;

        //need to subtract one as the call to getRange is inclusive
        //meaning if you want to download the first 10 bytes of a file, request bytes 0..9
        final long endByte = curEndByte - 1;
//        logger.info(String.format("start byte = %d, end byte = %d", firstByte, endByte));
        try
        {
            Integer cnt = new RetryableCallable<Integer>()
            {
                public Integer retriableCall() throws IOException
                {
                    GetObjectRequest req = new GetObjectRequest(bucketName, path.getRemotePath());
                    req.setRange(firstByte, endByte);
                    S3ObjectInputStream is = null;
                    try
                    {
                        is = s3Client.getObject(req).getObjectContent();

                        byte[] readBuf = new byte[4092];
                        int rCnt;
                        int readTotal = 0;
                        int incomingOffet = off;
                        while ((rCnt = is.read(readBuf, 0, readBuf.length)) >= 0)
                        {
                            System.arraycopy(readBuf, 0, b, incomingOffet, rCnt);
                            readTotal += rCnt;
                            incomingOffet += rCnt;
//                            logger.info("    local read cnt = " + rCnt + "Current Thread Name = "+Thread.currentThread().getName());
                        }
                        if(readTotal == 0 && rCnt == -1)
                        		return -1;
                        offset += readTotal;
                        return Integer.valueOf(readTotal);
                    }
                    finally
                    {
                        if(is != null)
                            is.close();
                    }
                }
            }.call();
//            logger.info("read cnt = " + cnt);
            return cnt.intValue();
        }
        catch(Exception e)
        {
            String msg = String.format("failed to read offset range %d-%d of file %s whose size is %d",
                    firstByte, endByte, path.getRemotePath(), path.getSize());
            throw new IOException(msg, e);
        }
    }

    public int read() throws IOException
    {
        logger.warn("read() called RangeReadInputStream");
        return -1;
    }
}

