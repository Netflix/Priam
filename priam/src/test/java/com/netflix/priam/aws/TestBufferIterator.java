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

package com.netflix.priam.aws;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.compress.CompressionType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

public class TestBufferIterator {
    private static final File DIR = new File("target/data/KS1/CF1/backups/201108082320/");
    private static final long SINGLE_CHUNK_MIN_SIZE = 4096;
    private static final long TWO_CHUNK_MIN_SIZE = 512;
    private static final int SINGLE_CHUNK_FILE_BYTES = 1024;
    // Must span exactly two chunks: > TWO_CHUNK_MIN_SIZE and < 2 * TWO_CHUNK_MIN_SIZE
    private static final int TWO_CHUNK_FILE_BYTES = 768;
    private static Injector injector;
    private static File singleChunkFile;
    private static File twoChunkFile;

    private final ThreadLocal<ByteBuffer> inputBuffer = new ThreadLocal<>();
    private final ThreadLocal<ByteBuffer> compressBuffer = new ThreadLocal<>();

    @BeforeClass
    public static void setUp() throws IOException {
        if (!DIR.exists()) DIR.mkdirs();
        injector = Guice.createInjector(new BRTestModule());

        singleChunkFile = new File(DIR, "single-chunk-Data.db");
        byte[] singleChunkData = new byte[SINGLE_CHUNK_FILE_BYTES];
        Arrays.fill(singleChunkData, (byte) 42);
        Files.write(singleChunkFile.toPath(), singleChunkData);

        twoChunkFile = new File(DIR, "two-chunk-Data.db");
        byte[] twoChunkData = new byte[TWO_CHUNK_FILE_BYTES];
        Arrays.fill(twoChunkData, (byte) 99);
        Files.write(twoChunkFile.toPath(), twoChunkData);
    }

    @AfterClass
    public static void cleanup() throws IOException {
        FileUtils.cleanDirectory(DIR);
    }

    @Test
    public void testSingleChunkNoCompression() throws IOException {
        RemoteBackupPath path = backupPath(singleChunkFile, CompressionType.NONE);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, SINGLE_CHUNK_MIN_SIZE)) {
            Assert.assertTrue(contentsMatch(singleChunkFile, drainIterator(iter), CompressionType.NONE));
        }
    }

    @Test
    public void testTwoChunksNoCompression() throws IOException {
        RemoteBackupPath path = backupPath(twoChunkFile, CompressionType.NONE);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, TWO_CHUNK_MIN_SIZE)) {
            Assert.assertTrue(contentsMatch(twoChunkFile, drainIterator(iter), CompressionType.NONE));
        }
    }

    @Test
    public void testSingleChunkWithCompression() throws IOException {
        RemoteBackupPath path = backupPath(singleChunkFile, CompressionType.SNAPPY);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, SINGLE_CHUNK_MIN_SIZE)) {
            Assert.assertTrue(contentsMatch(singleChunkFile, drainIterator(iter), CompressionType.SNAPPY));
        }
    }

    @Test
    public void testTwoChunksWithCompression() throws IOException {
        RemoteBackupPath path = backupPath(twoChunkFile, CompressionType.SNAPPY);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, TWO_CHUNK_MIN_SIZE)) {
            Assert.assertTrue(contentsMatch(twoChunkFile, drainIterator(iter), CompressionType.SNAPPY));
        }
    }

    @Test
    public void testZeroLengthFileNoCompression() throws IOException {
        File emptyFile = new File(DIR, "empty-Data.db");
        emptyFile.createNewFile();
        RemoteBackupPath path = backupPath(emptyFile, CompressionType.NONE);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, SINGLE_CHUNK_MIN_SIZE)) {
            Assert.assertTrue(contentsMatch(emptyFile, drainIterator(iter), CompressionType.NONE));
        }
    }

    @Test
    public void testZeroLengthFileWithCompression() throws IOException {
        File emptyFile = new File(DIR, "empty-snappy-Data.db");
        emptyFile.createNewFile();
        RemoteBackupPath path = backupPath(emptyFile, CompressionType.SNAPPY);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, SINGLE_CHUNK_MIN_SIZE)) {
            Assert.assertTrue(contentsMatch(emptyFile, drainIterator(iter), CompressionType.SNAPPY));
        }
    }

    @Test
    public void testIteratorExhaustedAfterAllChunksConsumed() throws IOException {
        RemoteBackupPath path = backupPath(twoChunkFile, CompressionType.NONE);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, TWO_CHUNK_MIN_SIZE)) {
            drainIterator(iter);
            Assert.assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testSnappyHeaderOccursOnceInSingleChunk() throws IOException {
        RemoteBackupPath path = backupPath(singleChunkFile, CompressionType.SNAPPY);
        byte[] compressed;
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, SINGLE_CHUNK_MIN_SIZE)) {
            compressed = drainIterator(iter);
        }
        Assert.assertEquals(1, countOccurrences(compressed, BufferIterator.SNAPPY_HEADER));
    }

    @Test
    public void testSnappyHeaderOccursOnceInTwoChunks() throws IOException {
        RemoteBackupPath path = backupPath(twoChunkFile, CompressionType.SNAPPY);
        byte[] compressed;
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, TWO_CHUNK_MIN_SIZE)) {
            compressed = drainIterator(iter);
        }
        Assert.assertEquals(1, countOccurrences(compressed, BufferIterator.SNAPPY_HEADER));
    }

    @Test
    public void testSnappyChunkBoundariesRoundTrip() throws IOException {
        RemoteBackupPath path = backupPath(twoChunkFile, CompressionType.SNAPPY);
        byte[] compressed;
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, TWO_CHUNK_MIN_SIZE)) {
            compressed = drainIterator(iter);
        }
        Assert.assertArrayEquals(Files.readAllBytes(twoChunkFile.toPath()), decompressSnappyJavaChunks(compressed));
    }

    @Test
    public void testThreadLocalIsolationAcrossThreads() throws Exception {
        RemoteBackupPath path1 = backupPath(singleChunkFile, CompressionType.NONE);
        RemoteBackupPath path2 = backupPath(twoChunkFile, CompressionType.NONE);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<byte[]> f1 = pool.submit(() -> {
            try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path1, SINGLE_CHUNK_MIN_SIZE)) {
                return drainIterator(iter);
            }
        });
        Future<byte[]> f2 = pool.submit(() -> {
            try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path2, TWO_CHUNK_MIN_SIZE)) {
                return drainIterator(iter);
            }
        });
        pool.shutdown();
        Assert.assertTrue(
                contentsMatch(singleChunkFile, f1.get(), CompressionType.NONE)
                && contentsMatch(twoChunkFile, f2.get(), CompressionType.NONE));
    }

    @Test
    public void testBufferReusedForSmallerFile() throws IOException {
        // singleChunkFile allocates a 1024-byte readBuffer; twoChunkFile's per-chunk size is 512
        RemoteBackupPath path1 = backupPath(singleChunkFile, CompressionType.NONE);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path1, SINGLE_CHUNK_MIN_SIZE)) {
            drainIterator(iter);
        }
        ByteBuffer bufferAfterFirst = inputBuffer.get();

        RemoteBackupPath path2 = backupPath(twoChunkFile, CompressionType.NONE);
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path2, TWO_CHUNK_MIN_SIZE)) {
            drainIterator(iter);
        }
        Assert.assertSame(bufferAfterFirst, inputBuffer.get());
    }

    @Test
    public void testPartialChannelReadsRetried() throws IOException {
        RemoteBackupPath path = backupPath(singleChunkFile, CompressionType.NONE);
        FileChannel realChannel = FileChannel.open(Paths.get(singleChunkFile.getAbsolutePath()), StandardOpenOption.READ);
        byte[] result;
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, SINGLE_CHUNK_MIN_SIZE, new OneByteAtATimeChannel(realChannel))) {
            result = drainIterator(iter);
        }
        Assert.assertTrue(contentsMatch(singleChunkFile, result, CompressionType.NONE));
    }

    @Test
    public void testIncompressibleContentRoundTrips() throws IOException {
        File randomFile = new File(DIR, "random-Data.db");
        byte[] data = new byte[SINGLE_CHUNK_FILE_BYTES];
        new Random(42).nextBytes(data);
        Files.write(randomFile.toPath(), data);
        RemoteBackupPath path = backupPath(randomFile, CompressionType.SNAPPY);
        byte[] compressed;
        try (BufferIterator iter = new BufferIterator(inputBuffer, compressBuffer, path, SINGLE_CHUNK_MIN_SIZE)) {
            compressed = drainIterator(iter);
        }
        Assert.assertTrue(contentsMatch(randomFile, compressed, CompressionType.SNAPPY));
    }

    private RemoteBackupPath backupPath(File file, CompressionType compression) {
        RemoteBackupPath path = injector.getInstance(RemoteBackupPath.class);
        path.parseLocal(file, BackupFileType.SST_V2);
        path.setCompression(compression);
        return path;
    }

    private byte[] drainIterator(BufferIterator iter) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (iter.hasNext()) {
            ByteBuffer buf = iter.next();
            byte[] chunk = new byte[buf.remaining()];
            buf.get(chunk);
            out.write(chunk);
        }
        return out.toByteArray();
    }

    // Mirrors lines 102-108 of S3FileSystem.downloadFileImpl
    private boolean contentsMatch(File original, byte[] processed, CompressionType compression) throws IOException {
        byte[] originalBytes = Files.readAllBytes(original.toPath());
        byte[] result;
        if (compression == CompressionType.NONE || processed.length == 0) {
            result = processed;
        } else {
            try (SnappyInputStream is = new SnappyInputStream(new ByteArrayInputStream(processed));
                 ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                IOUtils.copyLarge(is, os);
                result = os.toByteArray();
            }
        }
        return Arrays.equals(originalBytes, result);
    }

    private int countOccurrences(byte[] data, byte[] pattern) {
        int count = 0;
        outer:
        for (int i = 0; i <= data.length - pattern.length; i++) {
            for (int j = 0; j < pattern.length; j++) {
                if (data[i + j] != pattern[j]) continue outer;
            }
            count++;
        }
        return count;
    }

    // Decompresses a snappy-java stream by parsing each length-prefixed chunk individually
    private byte[] decompressSnappyJavaChunks(byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.position(BufferIterator.SNAPPY_HEADER.length);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (buf.hasRemaining()) {
            int chunkLength = buf.getInt();
            byte[] chunk = new byte[chunkLength];
            buf.get(chunk);
            out.write(Snappy.uncompress(chunk));
        }
        return out.toByteArray();
    }

    // FileChannel wrapper that returns exactly one byte per positional read to exercise the retry loop
    private static class OneByteAtATimeChannel extends FileChannel {
        private final FileChannel real;

        OneByteAtATimeChannel(FileChannel real) {
            this.real = real;
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            if (!dst.hasRemaining()) return 0;
            ByteBuffer single = ByteBuffer.allocate(1);
            int n = real.read(single, position);
            if (n > 0) {
                single.flip();
                dst.put(single);
            }
            return n;
        }

        @Override public int read(ByteBuffer dst) throws IOException { return real.read(dst); }
        @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return real.read(dsts, offset, length); }
        @Override public int write(ByteBuffer src) throws IOException { return real.write(src); }
        @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException { return real.write(srcs, offset, length); }
        @Override public long position() throws IOException { return real.position(); }
        @Override public FileChannel position(long newPosition) throws IOException { real.position(newPosition); return this; }
        @Override public long size() throws IOException { return real.size(); }
        @Override public FileChannel truncate(long size) throws IOException { real.truncate(size); return this; }
        @Override public void force(boolean metaData) throws IOException { real.force(metaData); }
        @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException { return real.transferTo(position, count, target); }
        @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { return real.transferFrom(src, position, count); }
        @Override public int write(ByteBuffer src, long position) throws IOException { return real.write(src, position); }
        @Override public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return real.map(mode, position, size); }
        @Override public FileLock lock(long position, long size, boolean shared) throws IOException { return real.lock(position, size, shared); }
        @Override public FileLock tryLock(long position, long size, boolean shared) throws IOException { return real.tryLock(position, size, shared); }
        @Override protected void implCloseChannel() throws IOException { real.close(); }
    }
}
