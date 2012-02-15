package com.netflix.priam.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.netflix.priam.backup.SnappyCompression;
import com.netflix.priam.utils.SystemUtils;

public class TestCompression
{

    @Before
    public void setup() throws UnsupportedEncodingException, IOException
    {
        File f = new File("/tmp/compress-test.txt");
        FileOutputStream stream = new FileOutputStream(f);
        for (int i = 0; i < 1 * 1000 * 1000; i++)
        {
            stream.write("This is a test... Random things happen... and you are responsible for it...\n".getBytes("UTF-8"));
            stream.write("The quick brown fox jumps over the lazy dog.The quick brown fox jumps over the lazy dog.The quick brown fox jumps over the lazy dog.\n".getBytes("UTF-8"));
        }
        IOUtils.closeQuietly(stream);
    }

    @After
    public void done()
    {
        File f = new File("/tmp/compress-test.txt");
        if (f.exists())
            f.delete();
    }

    void validateCompression(String uncompress, String compress)
    {
        File uncompressed = new File(uncompress);
        File compressed = new File(compress);
        assertTrue(uncompressed.length() > compressed.length());
    }

    @Test
    public void zip() throws IOException
    {
        BufferedInputStream source = null;
        FileOutputStream dest = new FileOutputStream("/tmp/compressed.zip");
        ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));
        byte data[] = new byte[2048];
        File file = new File("/tmp/compress-test.txt");
        FileInputStream fi = new FileInputStream(file);
        source = new BufferedInputStream(fi, 2048);
        ZipEntry entry = new ZipEntry(file.getName());
        out.putNextEntry(entry);
        int count;
        while ((count = source.read(data, 0, 2048)) != -1)
        {
            out.write(data, 0, count);
        }
        IOUtils.closeQuietly(out);
        validateCompression("/tmp/compress-test.txt", "/tmp/compressed.zip");
    }

    @Test
    public void unzip() throws IOException
    {
        BufferedOutputStream dest1 = null;
        BufferedInputStream is = null;
        ZipFile zipfile = new ZipFile("/tmp/compressed.zip");
        Enumeration e = zipfile.entries();
        while (e.hasMoreElements())
        {
            ZipEntry entry = (ZipEntry) e.nextElement();
            is = new BufferedInputStream(zipfile.getInputStream(entry));
            int c;
            byte d[] = new byte[2048];
            FileOutputStream fos = new FileOutputStream("/tmp/compress-test-out-0.txt");
            dest1 = new BufferedOutputStream(fos, 2048);
            while ((c = is.read(d, 0, 2048)) != -1)
            {
                dest1.write(d, 0, c);
            }
            IOUtils.closeQuietly(dest1);
            IOUtils.closeQuietly(is);
        }
        String md1 = SystemUtils.md5(new File("/tmp/compress-test.txt"));
        String md2 = SystemUtils.md5(new File("/tmp/compress-test-out-0.txt"));
        assertEquals(md1, md2);
    }

    @Test
    public void snappyCompress() throws IOException
    {
        FileInputStream fi = new FileInputStream("/tmp/compress-test.txt");
        SnappyOutputStream out = new SnappyOutputStream(new BufferedOutputStream(new FileOutputStream("/tmp/test0.snp")));
        BufferedInputStream origin = new BufferedInputStream(fi, 1024);
        byte data[] = new byte[1024];
        int count;
        while ((count = origin.read(data, 0, 1024)) != -1)
        {
            out.write(data, 0, count);
        }
        IOUtils.closeQuietly(origin);
        IOUtils.closeQuietly(fi);
        IOUtils.closeQuietly(out);

        validateCompression("/tmp/compress-test.txt", "/tmp/test0.snp");
    }

    @Test
    public void snappyDecompress() throws IOException
    {
        // decompress normally.
        SnappyInputStream is = new SnappyInputStream(new BufferedInputStream(new FileInputStream("/tmp/test0.snp")));
        byte d[] = new byte[1024];
        FileOutputStream fos = new FileOutputStream("/tmp/compress-test-out-1.txt");
        BufferedOutputStream dest1 = new BufferedOutputStream(fos, 1024);
        int c;
        while ((c = is.read(d, 0, 1024)) != -1)
        {
            dest1.write(d, 0, c);
        }
        IOUtils.closeQuietly(dest1);
        IOUtils.closeQuietly(is);

        String md1 = SystemUtils.md5(new File("/tmp/compress-test-out-1.txt"));
        String md2 = SystemUtils.md5(new File("/tmp/compress-test.txt"));
        assertEquals(md1, md2);
    }

    @Test
    public void compress() throws FileNotFoundException, IOException
    {
        SnappyCompression compress = new SnappyCompression();
        RandomAccessFile file = new RandomAccessFile(new File("/tmp/compress-test.txt"), "r");
        long chunkSize = 5L*1024*1024;
        Iterator<byte[]> it = compress.compress(file, chunkSize);
        FileOutputStream ostream = new FileOutputStream("/tmp/test1.snp");
        while (it.hasNext())
        {
            byte[] chunk = it.next();
            ostream.write(chunk);
        }
        IOUtils.closeQuietly(ostream);
        validateCompression("/tmp/compress-test.txt", "/tmp/test1.snp");
    }

    @Test
    public void decompress() throws FileNotFoundException, IOException
    {
        SnappyCompression compress = new SnappyCompression();
        compress.decompress(new FileInputStream("/tmp/test1.snp"), new FileOutputStream("/tmp/compress-test-out-2.txt"));
        String md1 = SystemUtils.md5(new File("/tmp/compress-test.txt"));
        String md2 = SystemUtils.md5(new File("/tmp/compress-test-out-2.txt"));
        assertEquals(md1, md2);
    }
}
