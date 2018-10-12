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

import com.google.inject.ImplementedBy;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

@ImplementedBy(SnappyCompression.class)
public interface ICompression {

    enum CompressionAlgorithm {
        SNAPPY,
        LZ4,
        NONE
    }

    /**
     * Uncompress the input stream and write to the output stream. Closes both input and output
     * streams
     */
    void decompressAndClose(InputStream input, OutputStream output) throws IOException;

    /**
     * Produces chunks of compressed data.
     *
     * @param is inputstream to be compressed.
     * @param chunkSize compress the stream and return it in parts of chunk
     * @return compressed byte array iterator
     * @throws IOException
     */
    Iterator<byte[]> compress(InputStream is, long chunkSize) throws IOException;
}
