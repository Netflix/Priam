package com.netflix.priam.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.backup.IncrementalBackup;

public class IncrementalBackuper
{
    private static final Logger logger = LoggerFactory.getLogger(Backuper.class);

    public static void main(String[] args)
    {
        try
        {
            Application.initialize();
            IncrementalBackup backuper = Application.injector.getInstance(IncrementalBackup.class);
            try
            {
                backuper.execute();
            } catch (Exception e)
            {
                logger.error("Unable to backup: ", e);
            }
        } finally
        {
            Application.shutdownAdditionalThreads();
        }
    }
}