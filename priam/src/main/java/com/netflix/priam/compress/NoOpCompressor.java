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

public class NoOpCompressor implements ICompressor {
    private final OutputStream outputStream;

    public NoOpCompressor(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(byte[] b, int byteOffset, int byteLength) throws IOException {
        outputStream.write(b, byteOffset, byteLength);
    }

    @Override
    public void finish() throws IOException {
        outputStream.flush();
    }

    @Override
    public void closeQuietly() {
        try {
            if (outputStream != null) outputStream.close();
        } catch (IOException e) {
        }
    }
}
