/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyOutputStream;

/** Byte iterator representing compressed data. Uses snappy compression */
public class ChunkedStream implements Iterator<byte[]> {
    private static final int BYTES_TO_READ = 2048;

    private boolean hasnext = true;
    private final ByteArrayOutputStream bos;
    private final SnappyOutputStream snappy;
    private final InputStream origin;
    private final long chunkSize;
    private final CompressionType compression;

    public ChunkedStream(InputStream is, long chunkSize) {
        this(is, chunkSize, CompressionType.NONE);
    }

    public ChunkedStream(InputStream is, long chunkSize, CompressionType compression) {
        this.origin = is;
        this.bos = new ByteArrayOutputStream();
        this.snappy = new SnappyOutputStream(bos);
        this.chunkSize = chunkSize;
        this.compression = compression;
    }

    @Override
    public boolean hasNext() {
        return hasnext;
    }

    @Override
    public byte[] next() {
        try {
            byte data[] = new byte[BYTES_TO_READ];
            int count;
            while ((count = origin.read(data, 0, data.length)) != -1) {
                switch (compression) {
                    case NONE:
                        bos.write(data, 0, count);
                        break;
                    case SNAPPY:
                        snappy.write(data, 0, count);
                        break;
                    default:
                        throw new IllegalArgumentException("Snappy compression only.");
                }
                if (bos.size() >= chunkSize) return returnSafe();
            }
            // We don't have anything else to read hence set to false.
            return done();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] done() throws IOException {
        if (compression == CompressionType.SNAPPY) snappy.flush();
        byte[] return_ = bos.toByteArray();
        hasnext = false;
        IOUtils.closeQuietly(snappy);
        IOUtils.closeQuietly(bos);
        IOUtils.closeQuietly(origin);
        return return_;
    }

    private byte[] returnSafe() throws IOException {
        byte[] return_ = bos.toByteArray();
        bos.reset();
        return return_;
    }

    @Override
    public void remove() {}
}
