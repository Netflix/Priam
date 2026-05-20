package com.netflix.priam.aws;

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.utils.ThreadLocalByteBuffer;
import org.xerial.snappy.Snappy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class BufferIterator implements Closeable {
    private static final int SNAPPY_CHUNK_SIZE = 4;
    static final byte[] SNAPPY_HEADER = new byte[] {
            (byte) 0x82, 'S', 'N', 'A', 'P', 'P', 'Y', (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01
    };
    private final ByteBuffer readBuffer;
    private final ByteBuffer compressBuffer;
    private final FileChannel channel;
    private final CompressionType compressionType;
    private final long fileSize;
    private final long chunkSize;
    private boolean headerApplied;
    private long position;

    BufferIterator(ThreadLocal<ByteBuffer> inputBuffer,
                   ThreadLocal<ByteBuffer> compressBuffer,
                   AbstractBackupPath path,
                   long minimumChunkSize) throws IOException {
        this(inputBuffer, compressBuffer, path, minimumChunkSize,
                FileChannel.open(Paths.get(path.getBackupFile().getAbsolutePath()), StandardOpenOption.READ));
    }

    @com.google.common.annotations.VisibleForTesting
    BufferIterator(ThreadLocal<ByteBuffer> inputBuffer,
                   ThreadLocal<ByteBuffer> compressBuffer,
                   AbstractBackupPath path,
                   long minimumChunkSize,
                   FileChannel fileChannel) throws IOException {
        Path localPath = Paths.get(path.getBackupFile().getAbsolutePath());
        fileSize = localPath.toFile().length();
        chunkSize = S3FileSystem.getChunkSize(localPath, minimumChunkSize);
        compressionType = path.getCompression();
        // Direct buffer use is obviated when compressing.
        boolean useDirectBuffer = compressionType != CompressionType.SNAPPY;
        readBuffer = ThreadLocalByteBuffer.get(inputBuffer, (int) Math.min(chunkSize, fileSize), useDirectBuffer);
        if (compressionType == CompressionType.SNAPPY) {
            int maxCompressedLength = Snappy.maxCompressedLength((int) chunkSize) + SNAPPY_HEADER.length + SNAPPY_CHUNK_SIZE;
            this.compressBuffer = ThreadLocalByteBuffer.get(compressBuffer, maxCompressedLength, false);
        } else {
            this.compressBuffer = null;
        }
        this.channel = fileChannel;
    }

    public boolean hasNext() {
        return position < fileSize;
    }

    public ByteBuffer next() throws IOException {
        long bytesToRead = Math.min(chunkSize, fileSize - position);
        readBuffer.clear();
        readBuffer.limit((int) bytesToRead);
        int bytesRead = 0;
        while (bytesRead < bytesToRead) {
            int read = channel.read(readBuffer, position + bytesRead);
            if (read == -1) break;
            bytesRead += read;
        }
        readBuffer.flip();
        position += bytesRead;
        if (compressionType != CompressionType.SNAPPY) {
            return readBuffer;
        }
        compressBuffer.clear();
        int offset = headerApplied ? SNAPPY_CHUNK_SIZE : SNAPPY_CHUNK_SIZE + SNAPPY_HEADER.length;
        int compressedSize = Snappy.compress(readBuffer.array(), 0, bytesRead, compressBuffer.array(), offset);
        compressBuffer.position(0);
        if (!headerApplied) {
            compressBuffer.put(SNAPPY_HEADER);
            headerApplied = true;
        }
        compressBuffer.putInt(compressedSize);
        compressBuffer.limit(compressedSize + offset);
        compressBuffer.position(0);
        return compressBuffer;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
