package com.priam;

import java.io.File;

import com.priam.backup.Consumer;

public class FakeCLConsumer implements Consumer
{

    private int bytecount = 0;
    private int filecount = 0;
    private int headercount = 0;

    @Override
    public void write(byte[] b, int offset, int len)
    {
        bytecount += len;
    }

    @Override
    public void setName(String fileName)
    {
        filecount++;

    }

    @Override
    public void copyFiles(File[] files)
    {
        headercount += files.length;

    }

    public int getByteCount()
    {
        return bytecount;
    }

    public int getFileCount()
    {
        return this.filecount;
    }

    public int getHeaderCount()
    {
        return this.headercount;
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void restore()
    {
        // TODO Auto-generated method stub
        
    }

}
