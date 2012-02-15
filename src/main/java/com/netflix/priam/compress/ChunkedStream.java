package com.netflix.priam.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyOutputStream;
/**
 * Byte iterator representing compressed data.
 * Uses snappy compression
 */
public class ChunkedStream implements Iterator<byte[]>
{
    private boolean hasnext = true;
    private ByteArrayOutputStream bos;
    private SnappyOutputStream compress;
    private InputStream origin;
    private long chunkSize;
    private static int BYTES_TO_READ = 2048;

    public ChunkedStream(InputStream is, long chunkSize) throws IOException
    {
        this.origin = is;
        this.bos = new ByteArrayOutputStream();
        this.compress = new SnappyOutputStream(bos);
        this.chunkSize = chunkSize;
    }

    @Override
    public boolean hasNext()
    {
        return hasnext;
    }

    @Override
    public byte[] next()
    {
        try
        {
            byte data[] = new byte[BYTES_TO_READ];
            int count;
            while ((count = origin.read(data, 0, data.length)) != -1)
            {
                compress.write(data, 0, count);
                if (bos.size() >= chunkSize)
                    return returnSafe();
            }
            // We don't have anything else to read hence set to false.
            return done();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private byte[] done() throws IOException
    {
        compress.flush();
        byte[] return_ = bos.toByteArray();
        hasnext = false;
        IOUtils.closeQuietly(compress);
        IOUtils.closeQuietly(bos);
        try
        {
            if (origin != null)
                origin.close();
        }
        catch (IOException ex)
        {
            // do nothing.
        }
        return return_;
    }

    private byte[] returnSafe() throws IOException
    {
        byte[] return_ = bos.toByteArray();
        bos.reset();
        return return_;
    }

    @Override
    public void remove()
    {
    }

}
