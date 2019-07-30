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
import net.jpountz.lz4.LZ4BlockOutputStream;

public class LZ4Compressor implements ICompressor {
    private final LZ4BlockOutputStream lz4Compress;

    public LZ4Compressor(OutputStream outputStream, int chunkSize) {
        this.lz4Compress = new LZ4BlockOutputStream(outputStream, chunkSize);
    }

    @Override
    public void write(byte[] b, int byteOffset, int byteLength) throws IOException {
        lz4Compress.write(b, byteOffset, byteLength);
    }

    @Override
    public void finish() throws IOException {
        lz4Compress.finish();
    }

    @Override
    public void closeQuietly() {
        try {
            if (lz4Compress != null) lz4Compress.close();
        } catch (IOException e) {
        }
    }
}
