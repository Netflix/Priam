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

import java.io.*;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;

/** Byte iterator representing compressed data. Uses snappy compression */
public class ChunkedStream implements Iterator<byte[]> {
    private boolean hasnext = true;
    private final ByteArrayOutputStream bos;
    private final InputStream origin;
    private final long chunkSize;
    private static final int BYTES_TO_READ = 2048;
    private final ICompressor compressor;

    public ChunkedStream(
            ICompression.CompressionAlgorithm compressionAlgorithm, InputStream is, long chunkSize)
            throws IOException {
        this.origin = is;
        this.bos = new ByteArrayOutputStream();
        this.chunkSize = chunkSize;
        switch (compressionAlgorithm) {
            case LZ4:
                compressor = new LZ4Compressor(bos, (int) chunkSize);
                break;
            case SNAPPY:
                compressor = new SnappyCompressor(bos);
                break;
            default:
                compressor = new NoOpCompressor(bos);
                break;
        }
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
                compressor.write(data, 0, count);
                if (bos.size() >= chunkSize) return returnSafe();
            }
            // We don't have anything else to read hence set to false.
            return done();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] done() throws IOException {
        compressor.finish();
        byte[] return_ = bos.toByteArray();
        hasnext = false;
        compressor.closeQuietly();
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
