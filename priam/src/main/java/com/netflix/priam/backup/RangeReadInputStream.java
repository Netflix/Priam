/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.backup;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.netflix.priam.utils.RetryableCallable;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of InputStream that will request explicit byte ranges of the target file. This
 * will make it easier to retry a failed read - which is important if we don't want to \ throw away
 * a 100Gb file and restart after reading 99Gb and failing.
 */
public class RangeReadInputStream extends InputStream {
    private static final Logger logger = LoggerFactory.getLogger(RangeReadInputStream.class);

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final long fileSize;
    private final String remotePath;
    private long offset;

    public RangeReadInputStream(
            AmazonS3 s3Client, String bucketName, long fileSize, String remotePath) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.fileSize = fileSize;
        this.remotePath = remotePath;
    }

    public int read(final byte b[], final int off, final int len) throws IOException {
        if (fileSize > 0 && offset >= fileSize) return -1;
        final long firstByte = offset;
        long curEndByte = firstByte + len;
        curEndByte = curEndByte <= fileSize ? curEndByte : fileSize;

        // need to subtract one as the call to getRange is inclusive
        // meaning if you want to download the first 10 bytes of a file, request bytes 0..9
        final long endByte = curEndByte - 1;
        try {
            return new RetryableCallable<Integer>() {
                public Integer retriableCall() throws IOException {
                    GetObjectRequest req = new GetObjectRequest(bucketName, remotePath);
                    req.setRange(firstByte, endByte);
                    try (S3ObjectInputStream is = s3Client.getObject(req).getObjectContent()) {
                        byte[] readBuf = new byte[4092];
                        int rCnt;
                        int readTotal = 0;
                        int incomingOffet = off;
                        while ((rCnt = is.read(readBuf, 0, readBuf.length)) >= 0) {
                            System.arraycopy(readBuf, 0, b, incomingOffet, rCnt);
                            readTotal += rCnt;
                            incomingOffet += rCnt;
                        }
                        if (readTotal == 0 && rCnt == -1) return -1;
                        offset += readTotal;
                        return readTotal;
                    }
                }
            }.call();
        } catch (Exception e) {
            String msg =
                    String.format(
                            "failed to read offset range %d-%d of file %s whose size is %d",
                            firstByte, endByte, remotePath, fileSize);
            throw new IOException(msg, e);
        }
    }

    public int read() throws IOException {
        logger.warn("read() called RangeReadInputStream");
        return -1;
    }
}
