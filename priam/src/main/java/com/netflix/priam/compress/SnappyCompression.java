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
package com.netflix.priam.compress;

import java.io.*;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;

/** Class to generate compressed chunks of data from an input stream using SnappyCompression */
public class SnappyCompression implements ICompression {
    private static final int BUFFER = 2 * 1024;

    @Override
    public void decompressAndClose(InputStream input, OutputStream output) throws IOException {
        try {
            decompress(input, output);
        } finally {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(output);
        }
    }

    private void decompress(InputStream input, OutputStream output) throws IOException {
        byte data[] = new byte[BUFFER];
        try (BufferedOutputStream dest1 = new BufferedOutputStream(output, BUFFER);
                SnappyInputStream is = new SnappyInputStream(new BufferedInputStream(input))) {
            int c;
            while ((c = is.read(data, 0, BUFFER)) != -1) {
                dest1.write(data, 0, c);
            }
        }
    }
}
