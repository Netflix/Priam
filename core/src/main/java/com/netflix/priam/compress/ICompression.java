package com.netflix.priam.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public interface ICompression {
    /**
     * Uncompress the input stream and write to the output stream.
     * Closes both input and output streams
     */
    public void decompressAndClose(InputStream input, OutputStream output) throws IOException;

    /**
     * Produces chunks of compressed data.
     */
    public Iterator<byte[]> compress(InputStream is, long chunkSize) throws IOException;
}
