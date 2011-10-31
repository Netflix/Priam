package com.priam.aws;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.priam.backup.Consumer;
import com.priam.conf.IConfiguration;
import com.priam.identity.IPriamInstanceFactory;
import com.priam.identity.InstanceIdentity;

public class EBSConsumer implements Consumer
{
    private static final Logger logger = LoggerFactory.getLogger(EBSConsumer.class);
    private File currentCLBackupFile;
    private BufferedOutputStream bos;
    private IConfiguration config;
    private File backupCLDir;
    private long errorCount;

    @Inject
    public EBSConsumer(IConfiguration config, InstanceIdentity identity, IPriamInstanceFactory factory) throws IOException, InterruptedException
    {
        this.backupCLDir = new File(config.getBackupCommitLogLocation());
        if (!backupCLDir.exists())
            backupCLDir.mkdirs();
        if (identity.getInstance().volumes == null || identity.getInstance().volumes.size() == 0)
            factory.attachVolumes(identity.getInstance(), backupCLDir.getAbsolutePath(), "/dev/sde6");

        this.config = config;
    }

    @Override
    public void write(byte[] b, int offset, int len)
    {
        try
        {
            bos.write(b, offset, len);
        }
        catch (IOException e)
        {
            if (errorCount % 256 == 0) // Dont obsess error logs
                logger.error("IOE: ", e);
            errorCount++;
        }
    }

    @Override
    public void setName(String fileName)
    {
        try
        {
            errorCount = 0;
            if (bos != null)
            {
                IOUtils.closeQuietly(bos);
            }
            if (!backupCLDir.exists())
                backupCLDir.mkdirs();
            currentCLBackupFile = new File(backupCLDir, fileName);

            FileOutputStream fio = new FileOutputStream(currentCLBackupFile, true);
            bos = new BufferedOutputStream(fio);
        }
        catch (IOException e)
        {
            logger.error("IOE: ", e);
        }

    }

    @Override
    public void copyFiles(File[] headerList)
    {
        try
        {
            for (File srcHeaderFile : headerList)
            {
                FileUtils.copyFileToDirectory(srcHeaderFile, backupCLDir);
            }
        }
        catch (IOException e)
        {
            logger.error("IOE: ", e);
        }
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(bos);
    }

    @Override
    public void restore()
    {
        try
        {
            File clDir = new File(config.getCommitLogLocation());
            if (!clDir.exists() || !clDir.isDirectory())
                throw new RuntimeException("Cannot find commit log location ");
            if (!backupCLDir.exists() || !backupCLDir.isDirectory())
                throw new RuntimeException("Cannot find backup commit log location ");

            File[] flist = backupCLDir.listFiles();
            for (File file : flist)
            {
                FileUtils.copyFileToDirectory(file, clDir);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
