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
import org.xerial.snappy.SnappyOutputStream;

public class SnappyCompressor implements ICompressor {
    private final SnappyOutputStream snappyCompress;

    public SnappyCompressor(OutputStream outputStream) {
        snappyCompress = new SnappyOutputStream(outputStream);
    }

    @Override
    public void write(byte[] b, int byteOffset, int byteLength) throws IOException {
        snappyCompress.write(b, byteOffset, byteLength);
    }

    @Override
    public void finish() throws IOException {
        snappyCompress.flush();
    }

    @Override
    public void closeQuietly() {
        try {
            if (snappyCompress != null) snappyCompress.close();
        } catch (IOException e) {
        }
    }
}
