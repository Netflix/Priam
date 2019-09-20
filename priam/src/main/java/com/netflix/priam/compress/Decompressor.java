package com.netflix.priam.compress;
/*
 * Copyright 2019 Netflix, Inc.
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

import java.io.*;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.xerial.snappy.SnappyInputStream;

public class Decompressor {
    private static final int BUFFER = 2 * 1024;

    public static void decompress(
            ICompression.CompressionAlgorithm compressionAlgorithm,
            InputStream input,
            OutputStream output)
            throws IOException {
        decompress(getInputStream(compressionAlgorithm, input), output);
    }

    private static InputStream getInputStream(
            ICompression.CompressionAlgorithm compressionAlgorithm, InputStream inputStream)
            throws IOException {
        switch (compressionAlgorithm) {
            case SNAPPY:
                return new SnappyInputStream(new BufferedInputStream(inputStream));
            case LZ4:
                return new LZ4BlockInputStream(new BufferedInputStream(inputStream));
            default:
                return new BufferedInputStream(inputStream);
        }
    }

    private static void decompress(InputStream input, OutputStream output) throws IOException {
        byte data[] = new byte[BUFFER];
        try (BufferedOutputStream dest1 = new BufferedOutputStream(output, BUFFER);
                InputStream is = input) {
            int c;
            while ((c = is.read(data, 0, BUFFER)) != -1) {
                dest1.write(data, 0, c);
            }
        }
    }
}
