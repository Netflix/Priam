package com.netflix.priam.compress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;

/**
 * Class to generate compressed chunks of data from an input stream using
 * SnappyCompression
 */
public class SnappyCompression implements ICompression
{
    private static final int BUFFER = 2 * 1024;

    @Override
    public Iterator<byte[]> compress(InputStream is, long chunkSize) throws IOException
    {
        return new ChunkedStream(is, chunkSize);
    }

    @Override
    public void decompressAndClose(InputStream input, OutputStream output) throws IOException
    {
        try
        {
            decompress(input, output);
        }
        finally
        {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(output);
        }
    }

    private void decompress(InputStream input, OutputStream output) throws IOException
    {
        SnappyInputStream is = new SnappyInputStream(new BufferedInputStream(input));
        byte data[] = new byte[BUFFER];
        BufferedOutputStream dest1 = new BufferedOutputStream(output, BUFFER);
        try
        {
            int c;
            while ((c = is.read(data, 0, BUFFER)) != -1)
            {
                dest1.write(data, 0, c);
            }
        }
        finally
        {
            IOUtils.closeQuietly(dest1);
            IOUtils.closeQuietly(is);
        }
    }
}
