package com.priam.backup;

import java.io.File;

/**
 * Should we exited Output stream instead?
 */
public interface Consumer
{
    public void write(byte[] b, int offset, int len);

    public void setName(String fileName);

    public void copyFiles(File[] files);
    
    public void close();
    
    public void restore();
}
