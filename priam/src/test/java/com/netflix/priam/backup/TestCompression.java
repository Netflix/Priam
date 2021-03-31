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

package com.netflix.priam.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.priam.compress.ChunkedStream;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.utils.SystemUtils;
import java.io.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCompression {

    private final File randomContentFile = new File("/tmp/content.txt");

    @Before
    public void setup() throws IOException {
        try (FileOutputStream stream = new FileOutputStream(randomContentFile)) {
            for (int i = 0; i < (5 * 5); i++) {
                stream.write(
                        "This is a test... Random things happen... and you are responsible for it...\n"
                                .getBytes("UTF-8"));
                stream.write(
                        "The quick brown fox jumps over the lazy dog.The quick brown fox jumps over the lazy dog.The quick brown fox jumps over the lazy dog.\n"
                                .getBytes("UTF-8"));
            }
        }
    }

    @After
    public void done() {
        FileUtils.deleteQuietly(randomContentFile);
    }

    @Test
    public void zipTest() throws IOException {
        String zipFileName = "/tmp/compressed.zip";
        File decompressedTempOutput = new File("/tmp/compress-test-out.txt");

        try {
            try (ZipOutputStream out =
                            new ZipOutputStream(
                                    new BufferedOutputStream(new FileOutputStream(zipFileName)));
                    BufferedInputStream source =
                            new BufferedInputStream(
                                    new FileInputStream(randomContentFile), 2048); ) {
                byte data[] = new byte[2048];
                ZipEntry entry = new ZipEntry(randomContentFile.getName());
                out.putNextEntry(entry);
                int count;
                while ((count = source.read(data, 0, 2048)) != -1) {
                    out.write(data, 0, count);
                }
            }
            assertTrue(randomContentFile.length() > new File(zipFileName).length());

            ZipFile zipfile = new ZipFile(zipFileName);
            Enumeration e = zipfile.entries();
            while (e.hasMoreElements()) {
                ZipEntry entry = (ZipEntry) e.nextElement();
                try (BufferedInputStream is =
                                new BufferedInputStream(zipfile.getInputStream(entry));
                        BufferedOutputStream dest1 =
                                new BufferedOutputStream(
                                        new FileOutputStream(decompressedTempOutput), 2048)) {
                    int c;
                    byte d[] = new byte[2048];

                    while ((c = is.read(d, 0, 2048)) != -1) {
                        dest1.write(d, 0, c);
                    }
                }
            }
            String md1 = SystemUtils.md5(randomContentFile);
            String md2 = SystemUtils.md5(decompressedTempOutput);
            assertEquals(md1, md2);
        } finally {
            FileUtils.deleteQuietly(new File(zipFileName));
            FileUtils.deleteQuietly(decompressedTempOutput);
        }
    }

    @Test
    public void snappyTest() throws IOException {
        ICompression compress = new SnappyCompression();
        testCompressor(compress);
    }

    private void testCompressor(ICompression compress) throws IOException {
        File compressedOutputFile = new File("/tmp/test1.compress");
        File decompressedTempOutput = new File("/tmp/compress-test-out.txt");
        long chunkSize = 5L * 1024 * 1024;
        try {

            Iterator<byte[]> it =
                    new ChunkedStream(
                            new FileInputStream(randomContentFile),
                            chunkSize,
                            CompressionType.SNAPPY);
            try (FileOutputStream ostream = new FileOutputStream(compressedOutputFile)) {
                while (it.hasNext()) {
                    byte[] chunk = it.next();
                    ostream.write(chunk);
                }
                ostream.flush();
            }

            assertTrue(randomContentFile.length() > compressedOutputFile.length());

            compress.decompressAndClose(
                    new FileInputStream(compressedOutputFile),
                    new FileOutputStream(decompressedTempOutput));
            String md1 = SystemUtils.md5(randomContentFile);
            String md2 = SystemUtils.md5(decompressedTempOutput);
            assertEquals(md1, md2);
        } finally {
            FileUtils.deleteQuietly(compressedOutputFile);
            FileUtils.deleteQuietly(decompressedTempOutput);
        }
    }
}
