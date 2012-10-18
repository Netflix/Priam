package com.netflix.priam.compress;

import com.google.common.io.CountingInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Class to generate compressed chunks of data from an input stream using
 * SnappyCompression
 */
public class SnappyCompression implements ICompression {

    @Override
    public Iterator<byte[]> compress(InputStream is, long chunkSize) throws IOException {
        return new ChunkedStream(is, chunkSize);
    }

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
        SnappyInputStream is = new SnappyInputStream(new BufferedInputStream(input));
        BufferedOutputStream dest1 = new BufferedOutputStream(output);
        try {
            IOUtils.copyLarge(is, dest1);
            dest1.flush();
            output.flush();
        } finally {
            IOUtils.closeQuietly(dest1);
            IOUtils.closeQuietly(is);
        }
    }
}
