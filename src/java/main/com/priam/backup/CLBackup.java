package com.priam.backup;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.priam.conf.IConfiguration;
import com.priam.scheduler.SimpleTimer;
import com.priam.scheduler.Task;
import com.priam.scheduler.TaskTimer;
import com.priam.utils.SystemUtils;

@Singleton
public class CLBackup extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(CLBackup.class);
    public static long SLEEP_INTERVAL = 10000L;
    public static final String JOBNAME = "CL_BACKUP_THREAD";
    private Consumer consumer;
    private IConfiguration config;
    private File currentCLFile;
    private RandomAccessFile raf;
    private long byteOffset;
    private static FilenameFilter headerFilter = new FilenameFilter()
    {
        public boolean accept(File dir, String name)
        {
            return !name.endsWith(".header");
        }
    };
    private static FilenameFilter logFilter = new FilenameFilter()
    {
        public boolean accept(File dir, String name)
        {
            return !name.endsWith(".log");
        }
    };

    @Inject
    public CLBackup(IConfiguration conf, Consumer consumer)
    {
        this.consumer = consumer;
        this.config = conf;
    }

    @Override
    public void execute() throws IOException
    {
        byte[] buf = new byte[2048];
        try
        {
            if (raf == null || raf.length() == raf.getFilePointer())
            {
                // Did the file rotate
                File newFile = getLatestFile();
                if (newFile == null)
                    return;
                if (currentCLFile == null || !newFile.equals(currentCLFile))
                    setCLFile(newFile);
            }

            int readBytes = 0;
            while ((readBytes = raf.read(buf, 0, buf.length)) != -1)
            {
                consumer.write(buf, 0, readBytes);
                byteOffset += readBytes;
                if (raf.getFilePointer() >= raf.length())
                    break;
            }
        }
        catch (IOException io)
        {
            try
            {
                if (raf != null)
                    raf.close();
                raf = null;
                if (consumer != null)
                    consumer.close();
            }
            catch (IOException ex)
            {
                logger.error("IO Exception while closing.", ex);
            }
        }
    }

    public File getLatestFile()
    {
        File cldir = new File(config.getCommitLogLocation());
        File[] files = cldir.listFiles(headerFilter);
        return files.length == 0 ? null : SystemUtils.sortByLastModifiedTime(files)[0];
    }

    public void setCLFile(File newFile) throws IOException
    {
        if (raf != null)
            raf.close();
        byteOffset = 0;
        currentCLFile = newFile;
        raf = new RandomAccessFile(currentCLFile, "r");
        consumer.setName(newFile.getName());
        copyHeaderFiles();
    }

    public void copyHeaderFiles()
    {
        File cldir = new File(config.getCommitLogLocation());
        File[] files = cldir.listFiles(logFilter);
        if (files.length > 0)
            consumer.copyFiles(files);
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, CLBackup.SLEEP_INTERVAL);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

}
