package com.priam;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.priam.backup.Consumer;

public class FakeCLConsumer implements Consumer
{

    private int bytecount = 0;
    private Set<String> filelist = new HashSet<String>();
    private int headercount = 0;

    @Override
    public void write(byte[] b, int offset, int len)
    {
        bytecount += len;
    }

    @Override
    public void setName(String fileName)
    {
        filelist.add(fileName);
    }

    @Override
    public void copyFiles(File[] files)
    {
        for( File f : files ){
            if( f.getAbsolutePath().endsWith(".header"))
            headercount++;
        }

    }

    public int getByteCount()
    {
        return bytecount;
    }

    public int getFileCount()
    {
        return this.filelist.size();
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
